import uvicorn


def main():
    uvicorn.run("server.app:app", host="localhost", port=8000, reload=True)


if __name__ == "__main__":
    main()
