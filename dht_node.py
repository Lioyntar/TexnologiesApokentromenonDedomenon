import hashlib
import json
import os
import socket
import threading
import time

try:
    from bplustree import BPlusTree
    HAS_BPLUSTREE = True
except ImportError:
    HAS_BPLUSTREE = False

def safe_remove_db(filepath):
    try:
        if os.path.exists(filepath): os.remove(filepath)
        if os.path.exists(filepath + "-wal"): os.remove(filepath + "-wal")
        return True
    except: return False

class Node:
    def __init__(self, ip, port, m=160):
        self.ip = ip
        self.port = port
        self.m = m
        self.id = self._generate_hash(f"{ip}:{port}")
        
        # Storage Setup
        folder_name = "node_storage"
        storage_dir = os.path.join(os.getcwd(), folder_name)
        os.makedirs(storage_dir, exist_ok=True)
        
        filename = f"storage_chord_{self.id}.db"
        self.db_filename = os.path.join(storage_dir, filename)
        
        if HAS_BPLUSTREE:
            if not safe_remove_db(self.db_filename):
                self.db_filename = os.path.join(storage_dir, f"storage_chord_{self.id}_{int(time.time())}.db")
            self.storage = BPlusTree(self.db_filename, order=50, key_size=32)
        else:
            self.storage = {} 

        # Chord State (Now storing dicts with IP/Port, not objects)
        self.finger_table = [None] * m
        # Self reference format
        self.node_info = {'id': self.id, 'ip': self.ip, 'port': self.port}
        
        self.successor = self.node_info
        self.predecessor = None
        
        # Networking
        self.running = True
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.daemon = True
        self.server_thread.start()

    def _generate_hash(self, key):
        key_bytes = key.encode('utf-8')
        sha1 = hashlib.sha1(key_bytes)
        return int(sha1.hexdigest(), 16)

    # --- NETWORKING HELPER ---
    def send_request(self, target_node, command, payload={}):
        if target_node['id'] == self.id:
            return self.handle_local_command(command, payload)
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5) # Timeout to prevent deadlocks
                s.connect((target_node['ip'], target_node['port']))
                msg = {'command': command, 'payload': payload}
                s.sendall(json.dumps(msg).encode('utf-8'))
                response = s.recv(4096 * 4) # Large buffer for data
                return json.loads(response.decode('utf-8'))
        except Exception as e:
            # print(f"[Error] Connection to {target_node['port']} failed: {e}")
            return None

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(5)
            while self.running:
                try:
                    conn, addr = s.accept()
                    threading.Thread(target=self.handle_client, args=(conn,)).start()
                except: break

    def handle_client(self, conn):
        with conn:
            try:
                data = conn.recv(4096 * 4)
                if not data: return
                msg = json.loads(data.decode('utf-8'))
                response = self.handle_local_command(msg['command'], msg['payload'])
                conn.sendall(json.dumps(response).encode('utf-8'))
            except Exception as e:
                # print(f"Server Error: {e}")
                conn.sendall(json.dumps({'status': 'error'}).encode('utf-8'))

    def handle_local_command(self, command, payload):
        if command == 'find_successor':
            return self.find_successor_local(payload['key'], payload.get('hops', 0))
        elif command == 'get_predecessor':
            return self.predecessor
        elif command == 'set_predecessor':
            self.predecessor = payload['node']
            return {'status': 'ok'}
        elif command == 'set_successor':
            self.successor = payload['node']
            return {'status': 'ok'}
        elif command == 'insert':
            return self.insert_local(payload['key'], payload['data'])
        elif command == 'lookup':
            return self.lookup_local(payload['key'])
        elif command == 'update':
            return self.insert_local(payload['key'], payload['data']) # Update is same as insert logic
        elif command == 'delete':
            return self.delete_local(payload['key'])
        elif command == 'notify':
            return self.notify(payload['node'])
        return {'error': 'unknown command'}

    # --- CHORD LOGIC ---

    def _is_between(self, key, n1, n2, inclusive_end=False):
        if n1 < n2:
            return (n1 < key < n2) if not inclusive_end else (n1 < key <= n2)
        else:
            return (n1 < key) or (key < n2) if not inclusive_end else (n1 < key) or (key <= n2)

    def find_successor_local(self, key, hops=0):
        # Case 1: Key is between me and my successor -> Successor is responsible
        if self._is_between(key, self.id, self.successor['id'], inclusive_end=True):
            return {'node': self.successor, 'hops': hops + 1}
        
        # Case 2: Forward to closest preceding node
        n_prime = self.closest_preceding_node(key)
        
        if n_prime['id'] == self.id:
            # If I am the closest, but key is not in (me, successor], then successor is responsible (loop around)
            return {'node': self.successor, 'hops': hops + 1}
        
        # Recursive RPC call
        result = self.send_request(n_prime, 'find_successor', {'key': key, 'hops': hops + 1})
        if result: return result
        return {'node': self.successor, 'hops': hops + 1} # Fallback

    def closest_preceding_node(self, key):
        for i in range(min(len(self.finger_table), 20) - 1, -1, -1):
            finger = self.finger_table[i]
            if finger and self._is_between(finger['id'], self.id, key):
                return finger
        return self.node_info

    def insert_key(self, title, data):
        key = self._generate_hash(title)
        # 1. Find responsible node
        res = self.find_successor_local(key)
        target = res['node']
        # 2. Send insert command to that node
        self.send_request(target, 'insert', {'key': key, 'data': data})

    def insert_local(self, key, data):
        if HAS_BPLUSTREE:
            data_str = json.dumps(data)
            self.storage[key] = data_str.encode('utf-8')
        else:
            self.storage[key] = data
        return {'status': 'ok'}

    def update_key(self, title, new_data):
        self.insert_key(title, new_data)

    def delete_key(self, title):
        key = self._generate_hash(title)
        res = self.find_successor_local(key)
        self.send_request(res['node'], 'delete', {'key': key})

    def delete_local(self, key):
        try:
            if HAS_BPLUSTREE:
                try: del self.storage[key]
                except: return {'status': 'not_found'}
            else:
                if key in self.storage: del self.storage[key]
            return {'status': 'ok'}
        except: return {'status': 'error'}

    def lookup_key(self, title):
        key = self._generate_hash(title)
        res = self.find_successor_local(key)
        target = res['node']
        final_res = self.send_request(target, 'lookup', {'key': key})
        return final_res['val'], res['hops'] + (final_res.get('hops', 0))

    def lookup_local(self, key):
        val = None
        if HAS_BPLUSTREE:
            try:
                val_bytes = self.storage.get(key)
                if val_bytes: val = json.loads(val_bytes.decode('utf-8'))
            except: pass
        else:
            val = self.storage.get(key)
        return {'val': val, 'hops': 0}

    # Simplified Stabilize for static setup
    def join(self, known_node):
        # Find my successor via known node
        res = known_node.send_request(known_node.node_info, 'find_successor', {'key': self.id})
        self.successor = res['node']
        # Notify successor
        self.send_request(self.successor, 'notify', {'node': self.node_info})

    def notify(self, node):
        if self.predecessor is None or self._is_between(node['id'], self.predecessor['id'], self.id):
            self.predecessor = node
        return {'status': 'ok'}
    
    def leave(self):
        # Transfer keys to successor
        if HAS_BPLUSTREE:
            try:
                for k, v in self.storage.items():
                    data = json.loads(v.decode('utf-8'))
                    self.send_request(self.successor, 'insert', {'key': k, 'data': data})
            except: pass
        
        # Notify successor to update predecessor (Simple Fix)
        self.send_request(self.successor, 'set_predecessor', {'node': self.predecessor})
        self.cleanup()

    def cleanup(self):
        self.running = False
        if HAS_BPLUSTREE:
            try: self.storage.close()
            except: pass