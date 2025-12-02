import os


def load_props(run_dotenv=False):
    if run_dotenv:
        print('*** Loading ENV variables from `.env` with dotenv ***')
        from dotenv import load_dotenv, find_dotenv
        load_dotenv(find_dotenv(usecwd=True))
    return dict(os.environ)
