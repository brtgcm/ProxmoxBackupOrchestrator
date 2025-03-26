This Python script is a backup orchestrator designed for distributed Proxmox VE environments. 
Its key feature is performing sequential backups across nodes, avoiding the default parallel execution of vzdump that can lead to performance issues and resource contention.
