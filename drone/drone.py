# =============================================================================
# drone.py — Drone Autônomo do Sistema de Drones Distribuído
# =============================================================================
#
# CORREÇÃO APLICADA:
#
#   BUG 4 — drone IP capturado do src_ip do pacote UDP (quebrava no restart):
#     O payload UDP agora inclui o campo "host" com socket.gethostname().
#     O broker usa esse campo em vez do src_ip do pacote para comandar
#     o drone via TCP. Assim o endereço é sempre o hostname Docker, que
#     é resolvido pelo DNS interno da rede bridge mesmo após restarts.
#
# PORTAS:
#   --porta  → porta TCP onde o drone recebe comandos dos brokers
#   5001 UDP → porta para onde os drones enviam seus avisos de estado
#
# THREADS:
#   register   → anuncia estado a todos os brokers a cada 3s (UDP unicast)
#   cmd_server → aguarda comandos de missão dos brokers (TCP)
#   status     → loga estado a cada 5s
# =============================================================================

import argparse
import json
import logging
import os
import random
import socket
import threading
import time

DRONE_UPDATE_PORT = 5001

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

def parse_brokers(raw: str) -> dict:
    result = {}
    for entry in raw.strip().split(","):
        parts = entry.split(":")
        if len(parts) < 4: continue
        bid, host, tport, rport = parts[0], parts[1], int(parts[2]), int(parts[3])
        result[bid] = {
            "host": host, 
            "drone_udp_port": tport + 1  # <--- Calcula a porta correta aqui
        }
    return result


class Drone:

    def __init__(self, drone_id: str, cmd_port: int):
        self.drone_id = drone_id
        self.cmd_port = cmd_port

        self._lock = threading.Lock()

        self.alocado   = False
        self.broker    = None
        self.missao    = None
        self.completas = 0

        self.brokers = parse_brokers(os.environ.get("BROKER_PEERS", ""))

        # BUG 4 FIX: resolve o hostname uma vez na inicialização
        # O hostname Docker é estável entre restarts do container
        self._hostname = socket.gethostname()

        self.log = logging.getLogger(f"drone.{drone_id}")
        self.log.info(
            f"Iniciado | cmd_port={cmd_port} | hostname={self._hostname} | "
            f"brokers={list(self.brokers.keys())}"
        )

    # ── Envio de estado para os brokers ──────────────────────────────────────

    def _notify(self, tipo: str = "drone_update"):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with self._lock:
            pkt = json.dumps({
                "tipo": tipo,
                "drone_id": self.drone_id,
                "porta": self.cmd_port,
                "alocado": self.alocado,
                "broker": self.broker,
                "host": self._hostname,
            }).encode()

        for info in self.brokers.values():
            try:
                # Envia para a porta específica calculada (tport + 1)
                sock.sendto(pkt, (info["host"], info["drone_udp_port"]))
            except Exception:
                pass
        sock.close()

    # ── Thread: register ─────────────────────────────────────────────────────

    def register(self):
        self.log.info("Iniciando heartbeat de segurança (30s)")
        while True:
            # Envia um "estou vivo" a cada 30 segundos apenas por precaução
            self._notify("drone_heartbeat")
            time.sleep(30.0)

    # ── Thread: cmd_server ───────────────────────────────────────────────────

    def cmd_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.cmd_port))
        srv.listen(5)
        self.log.info(f"Aguardando comandos TCP:{self.cmd_port}")

        while True:
            try:
                conn, _ = srv.accept()
                threading.Thread(target=self._handle_cmd, args=(conn,), daemon=True).start()
            except Exception as e:
                self.log.error(f"CMD server: {e}")

    def _handle_cmd(self, conn: socket.socket):
        try:
            f = conn.makefile("r")
            for linha in f:
                linha = linha.strip()
                if not linha:
                    continue
                cmd = json.loads(linha)
                self._process_cmd(cmd)
        except Exception:
            pass
        finally:
            conn.close()

    def _process_cmd(self, cmd: dict):
        tipo = cmd.get("cmd")
        if tipo == "MISSAO":
            with self._lock:
                if self.alocado: return
                self.alocado = True
                self.broker  = cmd.get("broker")
                self.missao  = cmd

            self.log.info(f"Missão recebida!")
            # EVENTO: Notifica imediatamente a mudança para OCUPADO
            self._notify("drone_occupied_event") 
            threading.Thread(target=self._executar_missao, daemon=True).start()

    # ── Execução e finalização de missão ─────────────────────────────────────

    def _executar_missao(self):
        duracao = random.uniform(8.0, 15.0)
        self.log.info(f"Missão iniciada — duração simulada: {duracao:.1f}s")

        inicio = time.time()
        while time.time() - inicio < duracao:
            time.sleep(2.0)
            elapsed = time.time() - inicio
            self.log.info(
                f"Em missão {elapsed:.0f}/{duracao:.0f}s | sensor={self.missao.get('sensor')}"
            )

        self._finalizar_missao()

    def _finalizar_missao(self):
        with self._lock:
            self.alocado   = False
            self.broker    = None
            self.missao    = None
            self.completas += 1

        self.log.info(f"Missão concluída — disponível")
        # EVENTO: Notifica imediatamente a mudança para DISPONÍVEL
        self._notify("drone_available_event")

    # ── Thread: status ────────────────────────────────────────────────────────

    def status(self):
        while True:
            time.sleep(5.0)
            with self._lock:
                estado    = f"ALOCADO ({self.broker})" if self.alocado else "DISPONÍVEL"
                completas = self.completas
            self.log.info(f"Estado: {estado} | Missões completas: {completas}")

    # ── Ponto de entrada ──────────────────────────────────────────────────────

    def run(self):
        threads = [
            ("register", self.register),
            ("cmd",      self.cmd_server),
            ("status",   self.status),
        ]
        for name, target in threads:
            threading.Thread(target=target, name=name, daemon=True).start()

        self.log.info("Drone ativo.")
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.log.info("Drone encerrado.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Drone autônomo de monitoramento")
    parser.add_argument("--id",    required=True,           help="ID único do drone")
    parser.add_argument("--porta", type=int, required=True, help="Porta TCP para comandos")
    args = parser.parse_args()
    Drone(args.id, args.porta).run()