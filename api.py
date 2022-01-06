from flask import Flask, request
from flask_restx import Resource, Api, fields, reqparse, abort, marshal, marshal_with
from configparser import ConfigParser
import psycopg2 as pg
from psycopg2 import extensions
from healthcheck import HealthCheck, EnvironmentDump
from prometheus_flask_exporter import PrometheusMetrics, RESTfulPrometheusMetrics
from prometheus_client import Counter, generate_latest
from fluent import sender, handler
import logging
from time import time
import json
import os
import subprocess
import socket
import requests

app = Flask(__name__)

# Load configurations from the config file
def load_configurations():
    app.config.from_file("config.json", load=json.load)

    with open("config.json") as json_file:
        data = json.load(json_file)
        # Override variables defined in the config file with the ones defined in the environment(if set)
        for item in data:
            if os.environ.get(item):
                app.config[item] = os.environ.get(item)


load_configurations()


@app.route("/")
def welcome():
    return "Welcome!"


custom_format = {
    "name": "%(name_of_service)s",
    "method": "%(crud_method)s",
    "traffic": "%(directions)s",
    "ip": "%(ip_node)s",
    "status": "%(status)s",
    "code": "%(http_code)s",
}
logging.basicConfig(level=logging.INFO)
l = logging.getLogger("Ocene")
h = handler.FluentHandler(
    "Ocene", host=app.config["FLUENT_IP"], port=int(app.config["FLUENT_PORT"])
)
formatter = handler.FluentRecordFormatter(custom_format)
h.setFormatter(formatter)
l.addHandler(h)

l.info(
    "Setting up Ocene App",
    extra={
        "name_of_service": "Ocene",
        "crud_method": None,
        "directions": None,
        "ip_node": None,
        "status": None,
        "http_code": None,
    },
)

api = Api(
    app,
    version="1.0",
    doc="/openapi",
    title="Narocniki API",
    description="Abstrakt Narocniki API",
    default_swagger_filename="openapi.json",
    default="Ocene CRUD",
    default_label="koncne tocke in operacije",
)
ocenaApiModel = api.model(
    "ModelOceno",
    {
        "id": fields.Integer(readonly=True, description="ID ocene"),
        "ime": fields.String(readonly=True, description="Ime narocnika"),
        "ocena": fields.String(readonly=True, description="Ocena aplikacije"),
    },
)
oceneApiModel = api.model(
    "ModelNarocnikov", {"ocene": fields.List(fields.Nested(ocenaApiModel))}
)
ns = api.namespace("Ocene CRUD", description="Ocene koncne tocke in operacije")
posodobiModel = api.model(
    "PosodobiOceno", {"atribut": fields.String, "vrednost": fields.String}
)


def connect_to_database():
    return pg.connect(
        database=app.config["PGDATABASE"],
        user=app.config["PGUSER"],
        password=app.config["PGPASSWORD"],
        port=app.config["DATABASE_PORT"],
        host=app.config["DATABASE_IP"],
        connect_timeout=3,
    )


# Kubernetes Liveness Probe (200-399 healthy, 400-599 sick)
def check_database_connection():
    conn = connect_to_database()
    if conn.poll() == extensions.POLL_OK:
        print("POLL: POLL_OK")
    if conn.poll() == extensions.POLL_READ:
        print("POLL: POLL_READ")
    if conn.poll() == extensions.POLL_WRITE:
        print("POLL: POLL_WRITE")
    l.info(
        "Healtcheck povezave z bazo",
        extra={
            "name_of_service": "Ocene",
            "crud_method": "healthcheck",
            "directions": "out",
            "ip_node": socket.gethostbyname(socket.gethostname()),
            "status": "success",
            "http_code": None,
        },
    )
    return True, "Database connection OK"


def application_data():
    l.info(
        "Application environmental data dump",
        extra={
            "name_of_service": "Ocene",
            "crud_method": "envdump",
            "directions": "out",
            "ip_node": socket.gethostbyname(socket.gethostname()),
            "status": "success",
            "http_code": None,
        },
    )
    return {"maintainer": "Matevž Morato", "git_repo": "https://github.com/Paketi-org/"}


class NarocnikModel:
    def __init__(self, id, ime, ocena):
        self.id = id
        self.ime = ime
        self.ocena = ocena


ocenePolja = {"id": fields.Integer, "ime": fields.String, "ocena": fields.String}


class Narocnik(Resource):
    def __init__(self, *args, **kwargs):
        self.table_name = "ocene"
        self.conn = connect_to_database()
        self.cur = self.conn.cursor()
        self.cur.execute(
            "select exists(select * from information_schema.tables where table_name=%s)",
            (self.table_name,),
        )
        if self.cur.fetchone()[0]:
            print("Table {0} already exists".format(self.table_name))
        else:
            self.cur.execute(
                """CREATE TABLE ocene (
                                id INT NOT NULL,
                                ime CHAR(25),
                                ocena CHAR(300)
                             )"""
            )

        self.parser = reqparse.RequestParser()
        self.parser.add_argument("id", type=int)
        self.parser.add_argument("ime", type=str)
        self.parser.add_argument("ocena", type=str)
        super(Narocnik, self).__init__(*args, **kwargs)

    @marshal_with(ocenaApiModel)
    @ns.response(404, "Ocena ni najdena")
    @ns.doc("Vrni oceno")
    def get(self, id):
        """
        Vrni podatke oceno glede na ID
        """
        l.info(
            "Zahtevaj oceno z ID %s" % str(id),
            extra={
                "name_of_service": "Ocene",
                "crud_method": "get",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )
        self.cur.execute("SELECT * FROM ocene WHERE id = %s" % str(id))
        row = self.cur.fetchall()

        if len(row) == 0:
            l.warning(
                "Ocena z ID %s ni bila najdena in ne bo izbrisana" % str(id),
                extra={
                    "name_of_service": "Ocene",
                    "crud_method": "get",
                    "directions": "out",
                    "ip_node": socket.gethostbyname(socket.gethostname()),
                    "status": "fail",
                    "http_code": 404,
                },
            )
            abort(404)

        d = {}
        for el, k in zip(row[0], ocenePolja):
            d[k] = el

        ocena = NarocnikModel(
            id=d["id"], ime=d["ime"].strip(), ocena=d["ocena"].strip()
        )

        l.info(
            "Vrni oceno z ID %s" % str(id),
            extra={
                "name_of_service": "Ocene",
                "crud_method": "get",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": "success",
                "http_code": 200,
            },
        )

        return ocena, 200

    @ns.doc("Izbrisi oceno")
    @ns.response(404, "Narocnik ni najden")
    @ns.response(204, "Narocnik izbrisan")
    def delete(self, id):
        """
        Izbriši oceno glede na ID
        """
        l.info(
            "Izbrisi ocenaa z ID %s" % str(id),
            extra={
                "name_of_service": "Ocene",
                "crud_method": "delete",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )
        self.cur.execute("SELECT * FROM ocene")
        rows = self.cur.fetchall()
        ids = []
        for row in rows:
            ids.append(row[0])

        if id not in ids:
            l.warning(
                "Ocena z ID %s ni bil najden in ne bo izbrisan" % str(id),
                extra={
                    "name_of_service": "Ocene",
                    "crud_method": "delete",
                    "directions": "out",
                    "ip_node": socket.gethostbyname(socket.gethostname()),
                    "status": "fail",
                    "http_code": 404,
                },
            )
            abort(404)
        else:
            self.cur.execute("DELETE FROM ocene WHERE id = %s" % str(id))
            self.conn.commit()

        l.info(
            "Ocena z ID %s izbrisan" % str(id),
            extra={
                "name_of_service": "Ocene",
                "crud_method": "delete",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": "success",
                "http_code": 204,
            },
        )

        return 204


class ListNarocnikov(Resource):
    def __init__(self, *args, **kwargs):
        self.table_name = "ocene"
        self.conn = connect_to_database()
        self.cur = self.conn.cursor()
        self.cur.execute(
            "select exists(select * from information_schema.tables where table_name=%s)",
            (self.table_name,),
        )
        if self.cur.fetchone()[0]:
            print("Table {0} already exists".format(self.table_name))
        else:
            self.cur.execute(
                """CREATE TABLE ocene (
                                id INT NOT NULL,
                                ime CHAR(25),
                                ocena CHAR(300)
                             )"""
            )

        self.parser = reqparse.RequestParser()
        self.parser.add_argument(
            "id", type=int, required=True, help="ID naročnika je obvezen"
        )
        self.parser.add_argument("ime", type=str, required=False, help="Ime uporabnika")
        self.parser.add_argument("ocena", type=str, required=False, help="Ocena")
        self.parser.add_argument(
            "id_uporabnika", type=int, required=False, help="ID uporabnika"
        )

        super(ListNarocnikov, self).__init__(*args, **kwargs)

    @ns.marshal_list_with(oceneApiModel)
    @ns.doc("Vrni vse ocenae")
    def get(self):
        """
        Vrni vse ocenae
        """
        l.info(
            "Zahtevaj vse ocenae",
            extra={
                "name_of_service": "Ocene",
                "crud_method": "get",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )
        self.cur.execute("SELECT * FROM ocene")
        rows = self.cur.fetchall()
        ds = {}
        i = 0
        for row in rows:
            ds[i] = {}
            for el, k in zip(row, ocenePolja):
                ds[i][k] = el
            i += 1

        ocene = []
        for d in ds:
            ocena = NarocnikModel(
                id=ds[d]["id"], ime=ds[d]["ime"].strip(), ocena=ds[d]["ocena"].strip()
            )
            ocene.append(ocena)

        l.info(
            "Vrni vse ocene",
            extra={
                "name_of_service": "Ocene",
                "crud_method": "get",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": "success",
                "http_code": 200,
            },
        )

        return {"ocene": ocene}, 200

    @marshal_with(ocenaApiModel)
    @ns.expect(ocenaApiModel)
    @ns.doc("Dodaj oceno")
    def post(self):
        """
        Dodaj novega oceno
        """
        l.info(
            "Dodaj novega oceno",
            extra={
                "name_of_service": "Ocene",
                "crud_method": "post",
                "directions": "in",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": None,
                "http_code": None,
            },
        )
        args = self.parser.parse_args()

        ime_uporabnika = None
        vir = app.config["UPORABNIKI_IP"] + "narocniki/" + str(args["id_uporabnika"])
        resp = requests.get(vir)
        if resp.status_code != 200:
            l.warning("Uprabnik z ID %s ni bil najden".format(args["id_uporabnika"]))
            abort(404)

        uporabniki_podatki = resp.json()
        ime_uporabnika = uporabniki_podatki["ime"]
        self.cur.execute(
            """INSERT INTO {0} (id, ime, ocena)
                VALUES ({1}, '{2}', '{3}')""".format(
                "ocene", args["id"], ime_uporabnika, args["ocena"]
            )
        )
        self.conn.commit()
        ocena = NarocnikModel(
            id=args["id"], ime=ime_uporabnika, ocena=args["ocena"].strip()
        )

        l.info(
            "Nov ocena dodan",
            extra={
                "name_of_service": "Ocene",
                "crud_method": "post",
                "directions": "out",
                "ip_node": socket.gethostbyname(socket.gethostname()),
                "status": "success",
                "http_code": 201,
            },
        )

        return ocena, 201


health = HealthCheck()
envdump = EnvironmentDump()
health.add_check(check_database_connection)
envdump.add_section("application", application_data)
app.add_url_rule("/healthcheck", "healthcheck", view_func=lambda: health.run())
app.add_url_rule("/environment", "environment", view_func=lambda: envdump.run())
api.add_resource(ListNarocnikov, "/ocene")
api.add_resource(Narocnik, "/ocene/<int:id>")
l.info(
    "Ocene App pripravljen",
    extra={
        "name_of_service": "Ocene",
        "crud_method": None,
        "directions": None,
        "ip_node": None,
        "status": None,
        "http_code": None,
    },
)

app.run(host="0.0.0.0", port=5013)
h.close()
