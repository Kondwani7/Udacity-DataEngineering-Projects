from configparser import ConfigParser

config = ConfigParser()

config["AWS"] = {
    "AWS_ACCESS_KEY_ID": "*****",
    "AWS_SECRET_ACCESS_KEY" : "*****"
}

with open("aws_local_creds.cfg", "w") as file:
    config.write(file)