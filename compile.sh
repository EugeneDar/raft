python3 -m venv myenv
source myenv/bin/activate
pip install grpcio-tools
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto
