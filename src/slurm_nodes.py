import pyslurm

def check_slurm_node_status():
    """Получает статус всех узлов в SLURM."""
    nodes = pyslurm.node()
    node_info = nodes.get()
    for node_name, info in node_info.items():
        print(f"Node: {node_name}, State: {info['state']}")

if __name__ == "__main__":
    check_slurm_node_status()