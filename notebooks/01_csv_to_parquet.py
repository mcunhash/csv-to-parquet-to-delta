# Python 3.13.3
# pip install pandas pyarrow

import os
import time
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

# =========================
# Parâmetros (ajuste aqui)
# =========================
csv_path = r"C:\caminho\para\seu\arquivo.csv"  # ajuste o caminho do CSV
output_dir = r"C:\caminho\para\saida\parquet_partes"  # pasta onde salvar as partes .parquet
max_file_size_mb = 100  # tamanho-alvo (comprimido) por arquivo parquet

# Cria diretório de saída
os.makedirs(output_dir, exist_ok=True)

# =========================
# Constantes e opções
# =========================
BYTES_PER_MB = 1024 * 1024
ESTIMATED_COMPRESSION_RATIO = 0.20  # ~80% de redução típica com snappy
MAX_MEMORY_BUFFER_BYTES = int(max_file_size_mb * BYTES_PER_MB / ESTIMATED_COMPRESSION_RATIO)

read_options = pv.ReadOptions(block_size=10 * 1024 * 1024, encoding="latin1")  # 10 MB por bloco
parse_options = pv.ParseOptions(delimiter=";")
convert_options = pv.ConvertOptions()

# =========================
# Funções auxiliares
# =========================
def batch_memory_size(batch: pa.RecordBatch) -> int:
"""Estima o uso de memória do batch somando os buffers de cada coluna."""
total = 0
for array in batch:
    bufs = array.buffers()
    if bufs:
        for buf in bufs:
            if buf is not None:
                total += buf.size
return total

# =========================
# Processamento
# =========================
table_reader = pv.open_csv(
csv_path,
read_options=read_options,
parse_options=parse_options,
convert_options=convert_options,
)

part_num = 1
current_batches = []
current_bytes_in_batches = 0
total_rows = 0

print("Iniciando processamento direto de CSV para Parquet particionado.")
print(f"CSV: {csv_path}")
print(f"Saída: {output_dir}")
print(f"Tamanho-alvo por arquivo (comprimido): ~{max_file_size_mb} MB")
print(f"Limite de buffer em memória (estimado): {MAX_MEMORY_BUFFER_BYTES / BYTES_PER_MB:.2f} MB")

start_time = time.time()

for batch in table_reader:
current_batches.append(batch)
current_bytes_in_batches += batch_memory_size(batch)

# Se o buffer em memória estimado atingiu o limite, grava um arquivo parquet
if current_bytes_in_batches >= MAX_MEMORY_BUFFER_BYTES:
    table_to_write = pa.Table.from_batches(current_batches)
    out_path = os.path.join(output_dir, f"parte_{part_num:03d}.parquet")

    pq.write_table(table_to_write, out_path, compression="snappy")

    actual_file_size_mb = os.path.getsize(out_path) / BYTES_PER_MB
    total_rows += table_to_write.num_rows

    print(
        f"[{part_num:03d}] gravado: {out_path} | "
        f"linhas={table_to_write.num_rows:,} | {actual_file_size_mb:.2f} MB"
    )

    # Próxima parte
    part_num += 1
    current_batches = []
    current_bytes_in_batches = 0

# Grava o restante que ficou no buffer
if current_batches:
table_to_write = pa.Table.from_batches(current_batches)
out_path = os.path.join(output_dir, f"parte_{part_num:03d}.parquet")

pq.write_table(table_to_write, out_path, compression="snappy")

actual_file_size_mb = os.path.getsize(out_path) / BYTES_PER_MB
total_rows += table_to_write.num_rows

print(
    f"[{part_num:03d}] gravado: {out_path} | "
    f"linhas={table_to_write.num_rows:,} | {actual_file_size_mb:.2f} MB"
)

end_time = time.time()
print(
f"Processamento concluído em {end_time - start_time:.2f} segundos | "
f"linhas totais={total_rows:,} | partes={part_num}"
)

# Uso opcional via linha de comando (mantendo seu estilo como padrão):
# python notebooks/01_csv_to_parquet.py "C:\entrada\arquivo.csv" "C:\saida\parquet_partes" 200
if __name__ == "__main__" and len(sys.argv) >= 3:
csv_path = sys.argv[1]
output_dir = sys.argv[2]
if len(sys.argv) >= 4:
    max_file_size_mb = int(sys.argv[3])
