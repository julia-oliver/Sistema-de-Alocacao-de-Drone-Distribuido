import argparse
import json
import logging
import os
import socket
import threading
import time
import uuid

# ── Constantes ────────────────────────────────────────────────────────────────

HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_MAX_MISS = 3
PEER_REFRESH       = 3.0
TOKEN_SEND_RETRY   = 2.0
TOKEN_DELIM = b"\n"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

# ── Parsing de BROKER_PEERS ───────────────────────────────────────────────────

def parse_peers(raw: str) -> dict:
    peers = {}
    if not raw: return peers
    for entry in raw.strip().split(","):
        entry = entry.strip()
        if not entry: continue
        parts = entry.split(":")
        if len(parts) < 4: continue
        bid, host, tport, rport = parts[0], parts[1], int(parts[2]), int(parts[3])
        peers[bid] = {
            "host":       host,
            "token_port": tport,
            "req_port":   rport,
            "ping_port":  tport + 100,
            "hb_port":    tport + 200,
            "drone_port": tport + 1, # Porta de drones dinâmica para evitar conflito
        }
    return peers

# ── Classe principal ──────────────────────────────────────────────────────────

class Broker:

    def __init__(self, broker_id: str, token_port: int, request_port: int):
        self.broker_id      = broker_id
        self.token_port     = token_port
        self.request_port   = request_port
        self.ping_port      = token_port + 100
        self.heartbeat_port = token_port + 200
        self.drone_update_port = token_port + 1 

        self._lock = threading.RLock()
        self.peers = parse_peers(os.environ.get("BROKER_PEERS", ""))
        self.known: dict[str, dict] = {}
        self.ring:  list[str] = []
        self.next:  dict | None = None
        self.drones: dict[str, dict] = {}
        self.queue:  list[dict] = []
        self.clock = 0

        self._token_event = threading.Event()
        self._token_data:  dict | None = None
        self._server_ready = threading.Event()

        self.log = logging.getLogger(f"broker.{broker_id}")

    # ── Relógio de Lamport ────────────────────────────────────────────────────

    def _tick(self) -> int:
        with self._lock:
            self.clock += 1
            return self.clock

    def _sync(self, received: int):
        with self._lock:
            self.clock = max(self.clock, received) + 1

    # ── Anel ─────────────────────────────────────────────────────────────────

    def _rebuild_ring(self):
        members = sorted(self.known.keys() | {self.broker_id})
        self.ring = members
        idx = members.index(self.broker_id)
        next_id = members[(idx + 1) % len(members)]

        if next_id == self.broker_id:
            self.next = None
            return

        info = self.known.get(next_id)
        if info:
            self.next = {
                "id":         next_id,
                "host":       info["host"],
                "token_port": info["token_port"],
                "hb_port":    info["hb_port"],
            }

    def _wait_for_ring(self):
        self.log.info("Aguardando peers para formar o anel...")
        while True:
            with self._lock:
                if len(self.ring) >= 2: break
            time.sleep(1.0)
        self.log.info(f"Anel formado: {self.ring}")

    # ── Thread: token_server ─────────────────────────────────────────────────

    def token_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.token_port))
        srv.listen(5)
        self.log.info(f"Token server TCP:{self.token_port}")
        self._server_ready.set()

        while True:
            try:
                conn, _ = srv.accept()
                threading.Thread(target=self._recv_token, args=(conn,), daemon=True).start()
            except Exception as e:
                self.log.error(f"Token server error: {e}")

    def _recv_token(self, conn: socket.socket):
        try:
            buf = b""
            conn.settimeout(5.0)
            while True:
                chunk = conn.recv(4096)
                if not chunk: break
                buf += chunk
                if TOKEN_DELIM in buf: break
            if buf:
                line = buf.split(TOKEN_DELIM)[0]
                token = json.loads(line.decode())
                with self._lock:
                    self._token_data = token
                self._token_event.set()
        except Exception as e:
            self.log.warning(f"Erro ao receber token: {e}")
        finally:
            conn.close()

    # ── Thread: ping_server ──────────────────────────────────────────────────

    def ping_server(self):
        self._server_ready.wait()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.ping_port))
        pong = json.dumps({"tipo": "pong_discover", "id": self.broker_id}).encode()
        while True:
            try:
                data, addr = sock.recvfrom(512)
                pkt = json.loads(data.decode())
                if pkt.get("tipo") == "ping_discover":
                    sock.sendto(pong, addr)
            except: pass

    # ── Thread: discovery ────────────────────────────────────────────────────

    def discovery(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(1.0)
        ping = json.dumps({"tipo": "ping_discover", "de": self.broker_id}).encode()
        while True:
            for bid, info in self.peers.items():
                if bid == self.broker_id: continue
                alive = False
                try:
                    sock.sendto(ping, (info["host"], info["ping_port"]))
                    data, _ = sock.recvfrom(512)
                    pkt = json.loads(data.decode())
                    alive = (pkt.get("tipo") == "pong_discover" and pkt.get("id") == bid)
                except: pass

                with self._lock:
                    if alive:
                        if bid not in self.known:
                            self.known[bid] = {**info, "ts": time.time()}
                            self.log.info(f"Broker {bid} descoberto")
                            self._rebuild_ring()
                        else:
                            self.known[bid]["ts"] = time.time()
            time.sleep(PEER_REFRESH)

    # ── Loop principal do token ───────────────────────────────────────────────

    def token_loop(self):
        self._server_ready.wait()
        self._wait_for_ring()

        # 1. Lógica inicial: Apenas o menor ID gera o primeiro token do sistema
        with self._lock:
            sou_o_menor = self.ring[0] == self.broker_id

        if sou_o_menor:
            self.log.info("Iniciando sistema: Gerando token inicial.")
            token_inicial = {"drones": {}, "ts": self._tick(), "de": self.broker_id}
            self._process_and_forward(token_inicial)

        # 2. Loop Único com Timeout (Recuperação de Falhas)
        while True:
            # Espera o token chegar via TCP. Se não chegar em 15s, ocorre o timeout.
            chegou = self._token_event.wait(timeout=15.0) 
            
            if chegou:
                # Caso NORMAL: Token recebido
                self._token_event.clear()
                with self._lock:
                    token = self._token_data
                    self._token_data = None
                
                if token:
                    self._process_and_forward(token)
            
            else:
                # Caso de FALHA: O token sumiu do anel (provavelmente alguém caiu com ele)
                with self._lock:
                    # Recalcula quem é o menor ID vivo no momento
                    self._rebuild_ring()
                    if not self.ring: continue
                    sou_o_menor = (self.ring[0] == self.broker_id)
                
                if sou_o_menor:
                    self.log.error("ALERTA: Token perdido detectado! Eu sou o novo mestre. Regenerando...")
                    # Cria um novo token com o estado atual que eu conheço dos drones
                    token_novo = {
                        "drones": dict(self.drones), 
                        "ts": self._tick(), 
                        "de": self.broker_id
                    }
                    # Coloca o token para circular
                    self._process_and_forward(token_novo)
                else:
                    # Eu não sou o menor ID, então apenas reseto o tempo e continuo esperando
                    # o menor ID regenerar o token.
                    self.log.warning("Token sumiu. Aguardando o menor ID regenerar...")

    def _process_and_forward(self, token: dict):
        self._sync(token.get("ts", 0))
        time.sleep(1.0) # Retenção para controle de fluxo
        
        with self._lock:
            token_drones = token.get("drones", {})
            
            # 1. Fusão de Dados (UDP Local + Token Global)
            for did, dinfo in token_drones.items():
                if did in self.drones:
                    # Se o meu dado local (UDP do Drone) diz que ele está DISPONÍVEL,
                    # eu atualizo o Token com essa informação. 
                    # O hardware sempre tem a razão sobre estar livre.
                    if self.drones[did].get("alocado") == False:
                        dinfo["alocado"] = False
                        dinfo["broker_mediador"] = "---"
                    
                    # Atualiza o conhecimento local com o que sobrou do Token
                    self.drones[did].update(dinfo)
                else:
                    # Drone novo que eu ainda não conhecia, mas está no Token
                    self.drones[did] = dinfo

            # 2. Tenta alocar para a fila local
            self._try_allocate()

            # 3. Print de Status (Baseado no estado atualizado)
            self._print_drones_status()
        
        # 4. Prepara o Token de saída com a "verdade" atualizada
        outbound = {
            "drones": dict(self.drones),
            "ts": self._tick(),
            "de": self.broker_id
        }

        # LOOP DE ENVIO COM RE-AVALIAÇÃO DE VIZINHO
        while True:
            with self._lock:
                # RE-CALCULA o anel toda vez que falha o envio
                self._rebuild_ring() 
                nxt = self.next

            if not nxt:
                self.log.warning("Sozinho no anel... aguardando vizinhos.")
                time.sleep(2.0)
                continue

            self.log.info(f"Tentando enviar token para {nxt['id']}...")
            if self._send_token(nxt, outbound):
                # Sucesso! Sai do loop e espera o próximo token chegar
                break
            
            # Se chegou aqui, o b3 (ou próximo) caiu.
            self.log.error(f"Nó {nxt['id']} inalcançável. Forçando rebuild e tentando próximo...")
            
            # Remove o nó problemático manualmente para acelerar o processo
            with self._lock:
                if nxt['id'] in self.known:
                    del self.known[nxt['id']]
            
            time.sleep(TOKEN_SEND_RETRY)

    def _send_token(self, nxt: dict, token: dict) -> bool:
        """Apenas tenta a conexão TCP. Retorna True se enviou, False se falhou."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0) # Não deixa o anel preso esperando um nó morto
            s.connect((nxt["host"], nxt["token_port"]))
            payload = json.dumps(token).encode() + TOKEN_DELIM
            s.sendall(payload)
            s.close()
            self.log.info(f"Token -> {nxt['id']} (sucesso)")
            return True
        except Exception as e:
            self.log.warning(f"Falha TCP ao enviar para {nxt['id']}: {e}")
            return False

    def _try_allocate(self):
        """
        Verifica a fila de sensores e aloca drones disponíveis.
        """
        with self._lock:
            if not self.queue:
                return

            # Ordena a fila por urgência (menor número = maior prioridade) e depois por tempo
            self.queue.sort(key=lambda x: (x.get("urgencia", 5), x.get("clock", 0)))

            # Itera sobre a fila tentando encontrar drones livres
            for req in list(self.queue):
                # Busca o primeiro drone que não está alocado
                drone_id = next(
                    (did for did, dinfo in self.drones.items() if not dinfo.get("alocado")), 
                    None
                )

                if drone_id:
                    # Alocação bem sucedida!
                    self.drones[drone_id]["alocado"] = True
                    self.drones[drone_id]["broker_mediador"] = self.broker_id
                    self._cmd_drone(drone_id, req['sensor_id'])
                    
                    self.log.info(f"ALOCADO: Drone {drone_id} designado para Sensor {req['sensor_id']}")
                    
                    # Remove da fila local
                    self.queue.remove(req)
                    
                    # Aqui você chamaria a função para avisar o drone/sensor
                    # self._notificar_missao(drone_id, req)
                else:
                    # Se não há drones livres, para de tentar para esta volta do token
                    self.log.info("Nenhum drone livre no momento.")
                    break
                
    def _print_drones_status(self):
        """Imprime o status dos drones (recursos) e a mediação dos brokers."""
        with self._lock:
            if not self.drones:
                self.log.info("=== [RECURSOS] Nenhum drone detectado na rede ===")
                return

            self.log.info("=== TABELA DE RECURSOS COMPARTILHADOS (DRONES) ===")
            self.log.info(f"{'ID Drone':<15} | {'Status':<10} | {'Broker Mediador':<15}")
            self.log.info("-" * 50)
            
            for did, info in self.drones.items():
                status = "EM USO" if info.get("alocado") else "DISPONÍVEL"
                # O broker_mediador é quem reservou o drone para o seu sensor
                mediador = info.get("broker_mediador", "---")
                self.log.info(f"{did:<15} | {status:<10} | {mediador:<15}")
            
            self.log.info(f"Sensores aguardando neste Broker: {len(self.queue)}")
            self.log.info("==================================================")

    def heartbeat_recv(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", self.heartbeat_port))
        while True:
            try:
                data, addr = sock.recvfrom(512)
                pkt = json.loads(data.decode())
                if pkt.get("tipo") == "ping":
                    resp = json.dumps({"tipo": "pong", "id": self.broker_id, "nonce": pkt.get("nonce")})
                    sock.sendto(resp.encode(), addr)
            except: pass

    def heartbeat_send(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self._lock:
                nxt = self.next
            if not nxt: continue
            
            sucesso = False
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.settimeout(1.0)
                s.sendto(json.dumps({"tipo":"ping"}).encode(), (nxt["host"], nxt["hb_port"]))
                data, _ = s.recvfrom(512)
                sucesso = True
                s.close()
            except:
                sucesso = False

            if not sucesso:
                self.log.warning(f"!!! Broker {nxt['id']} MORREU. Removendo do anel...")
                with self._lock:
                    if nxt['id'] in self.known:
                        del self.known[nxt['id']]
                    self._rebuild_ring() # Força o anel a pular o nó morto imediatamente
                    
    def drone_updates(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", self.drone_update_port))
        while True:
            try:
                data, _ = sock.recvfrom(2048)
                pkt = json.loads(data.decode())
                did = pkt["drone_id"]
                with self._lock:
                    self.drones[did] = pkt
            except: pass
            
    def _cmd_drone(self, drone_id, sensor_id):
        """Envia o comando TCP real para o drone iniciar a missão."""
        drone_info = self.drones.get(drone_id)
        if not drone_info: return

        try:
            # Usa o 'host' e a 'porta' que o drone enviou via UDP
            host = drone_info.get("host")
            port = drone_info.get("porta")
            
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect((host, port))
            
            msg = json.dumps({
                "cmd": "MISSAO",
                "broker": self.broker_id,
                "sensor": sensor_id
            }) + "\n"
            
            s.sendall(msg.encode())
            s.close()
            self.log.info(f"Comando de missão enviado para {drone_id}")
        except Exception as e:
            self.log.error(f"Falha ao enviar comando para drone {drone_id}: {e}")
            
    def requests(self):
        """Servidor TCP que recebe pedidos dos sensores."""
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.request_port))
        srv.listen(10)
        self.log.info(f"Requests TCP server ouvindo na porta {self.request_port}")

        while True:
            try:
                conn, addr = srv.accept()
                # Cria uma thread para cada novo sensor que conectar
                threading.Thread(target=self._handle_request, args=(conn,), daemon=True).start()
            except Exception as e:
                self.log.error(f"Erro no servidor de requests: {e}")
    
    def _handle_request(self, conn: socket.socket):
        """Processa a mensagem e fecha a conexão imediatamente (Modelo Assíncrono)."""
        try:
            # 1. Recebe os dados do sensor
            data = conn.recv(2048).decode().strip()
            if not data:
                conn.close()
                return
            
            pkt = json.loads(data)
            
            # 2. Sincroniza o relógio de Lamport
            self._sync(pkt.get("ts", 0))
            
            # 3. Cria o pedido para a fila (SEM o objeto 'conn')
            req = {
                "sensor_id": pkt["sensor_id"],
                "urgencia":  int(pkt.get("urgencia", 5)),
                "ocorrencia": pkt.get("ocorrencia", "N/A"),
                "clock":     self._tick(), 
            }
            
            with self._lock:
                self.queue.append(req)
                
            self.log.info(f"Pedido registrado: {req['sensor_id']} | Fila: {len(self.queue)}")
            
        except Exception as e:
            self.log.warning(f"Erro ao receber pedido: {e}")
        finally:
            # 4. FECHA A CONEXÃO AQUI: O sensor não fica mais esperando
            conn.close()

    def run(self):
        targets = [
            self.token_server, self.ping_server, self.discovery,
            self.heartbeat_recv, self.heartbeat_send, self.drone_updates, self.requests
        ]
        for target in targets:
            threading.Thread(target=target, daemon=True).start()
        
        self.token_loop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--tport", type=int, required=True)
    parser.add_argument("--rport", type=int, required=True)
    args = parser.parse_args()
    Broker(args.id, args.tport, args.rport).run()