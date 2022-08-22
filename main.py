import logging

from beamflow import application

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    application.App().run()
