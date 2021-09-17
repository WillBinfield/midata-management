from os import path
from app import MidataApp


def main():
    data_path = path.join(path.abspath(path.dirname(__file__)), "Data")
    app = MidataApp(data_path)
    app.execute()


if __name__ == "__main__":
    main()