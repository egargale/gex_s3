from prefect import flow, Flow

@flow(name="My Flow")
def my_flow() -> str:
    return "Hello, world!"