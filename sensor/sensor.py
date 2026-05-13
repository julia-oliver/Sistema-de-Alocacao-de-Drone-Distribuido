# =============================================================================
# sensor.py — Sensor Naval do Sistema de Drones Distribuído
# =============================================================================
#
# Nenhuma correção de bug aplicada neste arquivo.
# Os bugs corrigidos eram todos em broker.py e drone.py.
#
# RESPONSABILIDADE:
#   Simula um sensor de monitoramento marítimo. Periodicamente detecta
#   ocorrências e solicita um drone ao seu broker preferido. Se o broker
#   preferido estiver indisponível, faz failover para outro broker.
#
# DESCOBERTA DE BROKERS:
#   Usa UDP ping na ping_port de cada broker (token_port + 100) para
#   verificar disponibilidade — o mesmo mecanismo usado entre brokers.
#
# FAILOVER:
#   Cada sensor tem um broker preferido (--broker). Se ele cair, o sensor
#   automaticamente usa outro broker disponível. Quando o preferido volta,
#   o sensor retorna a ele automaticamente.
#
# THREADS:
#   discovery → verifica disponibilidade dos brokers periodicamente (UDP ping)
#   detect    → gera ocorrências e solicita drones
#   status    → loga estado a cada 5s
# =============================================================================

import argparse
import json
import logging
import os
import random
import socket
import threading
import time

REQUEST_TIMEOUT = 15.0
PROBE_INTERVAL  = 4.0

OCORRENCIAS = [
    ("Sinal de socorro detectado",           1),
    ("Embarcação civil à deriva",            1),
    ("Detecção de objeto não identificado",  2),
    ("Embarcação sem transponder ativo",     2),
    ("Falha de sinalização marítima",        2),
    ("Suspeita de bloqueio de rota",         3),
    ("Variação anormal de corrente",         3),
    ("Congestionamento em corredor",         3),
    ("Risco ambiental detectado",            4),
    ("Replanejamento de tráfego",            5),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)

def parse_brokers(raw: str) -> dict:
    result = {}
    for entry in raw.strip().split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) < 4:
            continue
        bid, host, tport, rport = parts[0], parts[1], int(parts[2]), int(parts[3])
        result[bid] = {
            "host":      host,
            "req_port":  rport,
            "ping_port": tport + 100,
        }
    return result


class Sensor:

    def __init__(self, sensor_id: str, broker_pref: str, intervalo: float):
        self.sensor_id   = sensor_id
        self.broker_pref = broker_pref
        self.intervalo   = intervalo

        self._lock = threading.Lock()

        self.all_brokers = parse_brokers(os.environ.get("BROKER_PEERS", ""))

        self.alive:   dict[str, dict] = {}
        self.ativo    = None
        self.failover = False

        self.enviados  = 0
        self.atendidos = 0
        self.clock     = 0

        self.log = logging.getLogger(f"sensor.{sensor_id}")
        self.log.info(f"Iniciado | preferido={broker_pref} intervalo={intervalo}s")

    def _tick(self) -> int:
        with self._lock:
            self.clock += 1
            return self.clock

    # ── Thread: discovery ────────────────────────────────────────────────────

    def discovery(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(1.5)

        ping = json.dumps({"tipo": "ping_discover", "de": self.sensor_id}).encode()

        while True:
            for bid, info in self.all_brokers.items():
                alive = False
                try:
                    sock.sendto(ping, (info["host"], info["ping_port"]))
                    data, _ = sock.recvfrom(256)
                    pkt = json.loads(data.decode())
                    alive = pkt.get("tipo") == "pong_discover" and pkt.get("id") == bid
                except socket.timeout:
                    pass
                except Exception:
                    pass

                with self._lock:
                    if alive:
                        era_morto = bid not in self.alive
                        self.alive[bid] = {**info, "ts": time.time()}

                        if era_morto and bid == self.broker_pref and self.failover:
                            self.ativo    = bid
                            self.failover = False
                            self.log.info(f"Broker preferido {bid} voltou — encerrando failover")

                        if self.ativo is None and bid == self.broker_pref:
                            self.ativo = bid
                            self.log.info(f"Broker preferido {bid} disponível")

                    else:
                        if bid in self.alive:
                            del self.alive[bid]
                            self.log.warning(f"Broker {bid} não responde — removido")
                            if self.ativo == bid:
                                self.ativo = None

            time.sleep(PROBE_INTERVAL)

    def _escolher_broker(self):
        with self._lock:
            if not self.alive:
                return None, None

            if self.broker_pref in self.alive:
                self.ativo    = self.broker_pref
                self.failover = False
                return self.broker_pref, self.alive[self.broker_pref]

            outros = [(bid, info) for bid, info in self.alive.items() if bid != self.broker_pref]
            if outros:
                bid, info = random.choice(outros)
                if not self.failover:
                    self.log.warning(f"Failover: {self.broker_pref} indisponível — usando {bid}")
                self.ativo    = bid
                self.failover = True
                return bid, info

            return None, None

    # ── Thread: detect ───────────────────────────────────────────────────────

    def detect(self):
        self.log.info("Aguardando brokers...")
        while True:
            with self._lock:
                tem = bool(self.alive)
            if tem:
                break
            time.sleep(1.0)

        self.log.info("Iniciando detecção de ocorrências")
        time.sleep(random.uniform(1.0, 3.0))

        while True:
            ocorrencia, urgencia = random.choice(OCORRENCIAS)
            self.log.info(f"Ocorrência detectada [{urgencia}]: {ocorrencia}")
            self._solicitar(ocorrencia, urgencia)
            time.sleep(self.intervalo + random.uniform(-2.0, 2.0))

    def _solicitar(self, ocorrencia: str, urgencia: int):
        bid, info = self._escolher_broker()
        if not bid:
            self.log.warning("Nenhum broker disponível — pedido descartado")
            return

        pedido = json.dumps({
            "sensor_id":  self.sensor_id,
            "ocorrencia": ocorrencia,
            "urgencia":   urgencia,
            "ts":         self._tick(),
        }).encode()

        flag = "[FAILOVER] " if self.failover else ""
        self.log.info(f"→ {bid} {flag}urgência={urgencia}")

        sucesso = self._enviar(bid, info, pedido)

        if not sucesso:
            with self._lock:
                self.ativo = None
            bid2, info2 = self._escolher_broker()
            if bid2 and bid2 != bid:
                self.log.warning(f"Tentando fallback via {bid2}")
                sucesso = self._enviar(bid2, info2, pedido)

        with self._lock:
            self.enviados += 1
            if sucesso:
                self.atendidos += 1

    def _enviar(self, bid: str, info: dict, pedido: bytes) -> bool:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(REQUEST_TIMEOUT)
            s.connect((info["host"], info["req_port"]))
            s.sendall(pedido)
            
            # Não fazemos mais s.recv()! 
            # O sensor apenas entrega o dado e encerra a função.
            s.close()
            
            self.log.info(f"✓ Pedido entregue ao {bid}")
            return True

        except Exception as e:
            self.log.warning(f"Falha ao enviar para {bid}: {e}")
            return False
        
    # ── Thread: status ────────────────────────────────────────────────────────

    def status(self):
        while True:
            time.sleep(5.0)
            with self._lock:
                ativo    = self.ativo or "—"
                modo     = "FAILOVER" if self.failover else "normal"
                vivos    = list(self.alive.keys())
                enviados = self.enviados
                atend    = self.atendidos
            self.log.info(
                f"Broker: {ativo} [{modo}] | "
                f"Pedidos: {enviados} enviados / {atend} atendidos | "
                f"Vivos: {vivos}"
            )

    # ── Ponto de entrada ──────────────────────────────────────────────────────

    def run(self):
        threads = [
            ("disc",   self.discovery),
            ("detect", self.detect),
            ("status", self.status),
        ]
        for name, target in threads:
            threading.Thread(target=target, name=name, daemon=True).start()

        self.log.info("Sensor ativo.")
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.log.info("Sensor encerrado.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor de monitoramento naval")
    parser.add_argument("--id",        required=True,            help="ID único do sensor")
    parser.add_argument("--broker",    required=True,            help="ID do broker preferido")
    parser.add_argument("--intervalo", type=float, default=10.0, help="Segundos entre detecções")
    args = parser.parse_args()
    Sensor(args.id, args.broker, args.intervalo).run()