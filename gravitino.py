from gravitino import GravitinoClient

# Connect to Gravitino server
client = GravitinoClient("http://localhost:8090")

# Optional: Configure authentication
client = GravitinoClient(
    "http://localhost:8090",
    auth_type="basic",
    username="admin",
    password="password"
)