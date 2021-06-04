# /usr/bin python3

import asyncio
import asyncpg as apg
import configparser
import json
import logging
import site
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from functools import partial
from getpass import getpass, getuser
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import asyncio_mqtt as mq
import keyring
import pandas as pd
from aioconveyor import AioConveyor, Event
from . import amber_data as ad
from . import datacomposer as dc
from .dateawarejsonenc import DateAwareJSONEncoder

log = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

GST = 1.1
URL = "https://api.amberelectric.com.au/prices/listprices"
DATA_DIR = Path("../data/")
DATA_STEM = Path("amber")
# forecast updated every 5 minutes
LOOP_INTERVAL = 300
# delay to allow forecasts to be calcuated
LOOP_OFFSET = 150
CONFIG_FILENAME = "amber.ini"
NEM_TZ = "Australia/Brisbane"


current_row = int


# @dataclass
# class AmberPayload:
#     raw_dict: Dict  # decoded JSON dict
#     df: pd.DataFrame


def fetch_data(postcode: str) -> Dict[str, Any]:
    cmd = [
        "curl",
        "-X",
        "POST",
        URL,
        "-H",
        "Content-Type: application/json",
        "-d",
        f'{{"postcode": "{postcode}"}}',
    ]
    log.info(cmd)
    cp = subprocess.run(args=cmd, capture_output=True)
    return json.loads(cp.stdout)


def to_timeseries(data: Dict[str, Any], rounding: Optional[int] = 3) -> pd.DataFrame:
    postcode = data["postcode"]
    # currentNEMtime = data["currentNEMtime"]  # brisbane tz
    # networkProvider = data["networkProvider"]
    # E2 = staticPrices["E2"]
    timeseries = pd.DataFrame(data["variablePricesAndRenewables"])
    timeseries.drop(columns=["forecastedAt+period"], inplace=True)
    log.info(f"length of timeseries: {len(timeseries)}")
    for col in [
        "semiScheduledGeneration",
        "operationalDemand",
        "rooftopSolar",
        "wholesaleKWHPrice",
        "renewablesPercentage",
        "percentileRank",
        "usage",
    ]:
        try:
            timeseries[col] = timeseries[col].astype(float)
        except KeyError:
            log.warning(
                f"column '{col}' not found when trying to convert to float."
                "Column will be missing for insert"
            )
            # timeseries[col] = None
    timeseries["postcode"] = postcode
    for col in ("createdAt", "period", "forecastedAt"):
        timeseries[col] = pd.to_datetime(timeseries[col])
        timeseries[col] = timeseries[col].dt.tz_localize(NEM_TZ)
    staticPrices = data["staticPrices"]
    E1 = staticPrices["E1"]
    # formula from Amber
    timeseries["usage_price"] = (
        float(E1["totalfixedKWHPrice"])
        + float(E1["lossFactor"]) * timeseries.wholesaleKWHPrice
    )
    B1 = staticPrices["B1"]
    timeseries["export_price"] = (
        float(B1["totalfixedKWHPrice"])
        - float(B1["lossFactor"]) * timeseries.wholesaleKWHPrice
    )
    return timeseries.round(rounding)


def get_db_args(defaults: Optional[Dict[str, str]] = None) -> ArgumentParser:
    user = getuser()
    ap = ArgumentParser(add_help=False)
    ap.add_argument(
        "--host",
        default=None,
        help="Postgres remote host. "
        "Provide an empty string to override any config file setting if you want to test "
        "without writing to a database. In this case the data will be printed to stdout",
    )
    ap.add_argument("--user", default=user, help="Override unix username.")
    ap.add_argument(
        "--password",
        help="Supply database password on command line. This is not recommended.",
    )
    ap.add_argument(
        "--database",
        default=ad.DB,
        help="Override default postgres database name, normally only for test purposes.",
    )
    ap.add_argument(
        "--actual-30min-table",
        default=ad.TABLE_RAW_ACTUAL_30MIN,
        help="Override table name, normally only for test purposes.",
    )
    ap.add_argument(
        "--actual-5min-table",
        default=ad.TABLE_RAW_ACTUAL_5MIN,
        help="Override table name, normally only for test purposes.",
    )
    ap.add_argument(
        "--forecast-table",
        default=ad.TABLE_RAW_FORECAST,
        help="Override table name, normally only for test purposes.",
    )
    ap.add_argument(
        "--no-timescaledb",
        action="store_true",
        help="don't call create_hypertable() when the table is created."
        "See https://www.timescale.com/ for how this can help timeseries apps scale.",
    )
    if defaults is not None:
        ap.set_defaults(**defaults)
    return ap


def get_common_args(defaults: Optional[Dict[str, str]] = None) -> ArgumentParser:
    ap = ArgumentParser(add_help=False)
    ap.add_argument("--log-level", default="INFO", help="Override log level")
    if defaults is not None:
        ap.set_defaults(**defaults)
    return ap


def find_config_file(config_filename: Path) -> Path:
    locations: Sequence[str] = [
        loc for loc in (site.USER_BASE, sys.prefix, "/etc") if loc is not None
    ]
    for loc in locations:
        config_path = Path(loc) / CONFIG_FILENAME
        if config_path.exists():
            return config_path
    raise FileNotFoundError(f"{config_filename} not found in {', '.join(locations)}")


def get_args() -> Namespace:
    config = configparser.ConfigParser()
    config.read_dict({"db": {}, "common": {}})
    try:
        config_path = find_config_file(Path(CONFIG_FILENAME))
        with open(config_path, "r") as cf:
            config.read_file(cf)
        log.info(f"using config from: '{config_path}'")
    except FileNotFoundError as e:
        log.info(f"{str(e)},\nusing defaults, see --help for more info.")

    ap = ArgumentParser(
        parents=[
            get_db_args(defaults=dict(config["db"])),
            get_common_args(defaults=dict(config["common"])),
        ],
        epilog=(f"If '{CONFIG_FILENAME}' is present, it will be used for defaults. "),
    )
    ap.add_argument("--postcode", default="3133", help="select amber postcode")
    ap.add_argument(
        "--loop-interval",
        default=LOOP_INTERVAL,
        type=int,
        help="Device read interval. Rounded to the nearest second.",
    )
    ap.add_argument(
        "--loop-offset", default=LOOP_OFFSET, type=int, help="Device read offset."
    )
    ap.add_argument("--mq-host", help="Specifying a host turns on mqtt writer.")
    ap.add_argument(
        "--mq-topic",
        default="amber/{series}",  # series: actual_5min, actual_30min, forecast
        help="MQTT topic, also needs --mq-host.",
    )
    ap.add_argument(
        "--syslog-host",
        help="Set this string to a syslog host to turn on syslog. "
        "Set it to the empty string to disable any setting in the config file.",
    )
    ap.add_argument(
        "--syslog-port",
        default="514/udp",
        help="Set this string to a syslog port to turn on syslog (%(default)s). "
        "Can also specify protocol, eg 514/tcp",
    )
    ap.add_argument("--write-json", action="store_true", help="write the 'raw' json data")
    opt = ap.parse_args()
    if not opt.password:
        opt.password = get_password(user=opt.user)
    return opt


def get_password(
    user: str, password: Optional[str] = None, account: str = "postgres"
) -> str:
    """try to pull password from keyring or prompt

    The python-keyring package supplies this and is dependent on a compatible
    backend. For desktop use gnome has it covered.

    Args:
        user (str): name of account user
        password (Optional[str]): optional, although if you provide one it
            gets returned as is.
        account: this is an arbitrary string used in context like
            `keyring get <user> <account>`

    Returns:
        str: password for account
    """
    if not password:
        try:
            password = keyring.get_password(user, account)
        except Exception:
            password = None
        if not password:
            password = getpass("enter password: ")
    return password


async def produce(event: Event, postcode: str) -> Dict:
    raw_dict = fetch_data(postcode)
    return raw_dict


# async def json_consume(
#     event: Event, payload: AmberPayload, filename: Optional[Path] = None
# ) -> int:
#     if payload.raw_dict is None:
#         return -1
#     if filename is None:
#         filename = DATA_DIR / f"{DATA_STEM}_{datetime.now().isoformat()}"
#     path = filename.with_suffix(".json")
#     log.info(f"writing raw json to filename: '{path}'")
#     with open(path, "w") as j:
#         j.write(json.dumps(payload.raw_dict))
#     return 0


# async def csv_consume(event: Event, payload: AmberPayload) -> int:
#     for table_name, (df, _) in payload.tabular.items():
#         filename = DATA_DIR / f"{DATA_STEM}_{table_name}_{datetime.now().isoformat()}"
#         path = filename.with_suffix(".csv")
#         log.info(f"writing CSV to filename: '{path}'")
#         df.to_csv(path)
#     return 0


class MqttConsumer:
    def __init__(self, host: str, topic_template: str) -> None:
        self.host = host
        self.topic_template = topic_template
        self.enc = DateAwareJSONEncoder()

    async def start_client(self) -> mq.Client:
        self.client = mq.Client(self.host)
        await self.client.connect()
        return self.client

    async def consume(self, event: Event, payload: Dict) -> int:
        if event.loop_counter < 1:
            try:
                await self.start_client()
            except Exception:
                log.exception("mqtt: failed to initialize connection/tables")
                raise

        # create raw dataframe
        data = payload["data"]
        ts = to_timeseries(data)
        ts[ad.TIME_FIELD] = event.event_time

        for spec in ad.table_specs:
            df = spec.do_calculate(ts)
            if df is None or df.empty:
                log.info("mqtt table: %s is empty", spec.table_name)
                continue
            if len(df) > 1:
                log.info("mqtt multiple rows not implemented yet %s", spec.table_name)
                continue

            try:
                d = df.squeeze().dropna().to_dict()
                msg = self.enc.encode(d)
                await self.client.publish(
                    self.topic_template.format(series=spec.table_name), msg, qos=1
                )
            except Exception:
                log.exception("mqtt actual_5min publish failed")
        return 0


class PostgresConsumer:
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
    ):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn: apg.Connection

    async def connect(self):
        if self.password is None:
            raise Exception("database password must not be None")
        if self.host is None:
            raise Exception("database host must not be None")
        user = getuser() if self.user is None else self.user
        log.info(f"user: {user} db: {self.database} host: {self.host}")
        self.conn = await apg.connect(
            user=self.user, database=self.database, host=self.host, password=self.password
        )

    async def create_tables(self):
        for spec in ad.table_specs:
            create_sql = dc.compose_create(
                table_name=spec.table_name,
                fields=spec.field_map,
                primary_key=spec.primary_key,
                extra_sql=spec.extra_sql,
            )
            try:
                log.debug("create table: %s with \n %s", spec.table_name, create_sql)
                await self.conn.execute(create_sql)
            except Exception:
                log.exception("failed to create table: %s", create_sql)

    async def write(self, event: Event, payload: Dict) -> int:
        if event.loop_counter < 1:
            try:
                await self.connect()
                await self.create_tables()
            except Exception:
                log.exception("PostgresWriter: failed to initialize connection/tables")
                raise

        # create raw dataframe
        data = payload["data"]
        ts = to_timeseries(data)
        ts[ad.TIME_FIELD] = event.event_time

        try:
            for spec in ad.table_specs:
                df = spec.do_calculate(ts)
                if df is None or df.empty:
                    log.info("table: %s is empty", spec.table_name)
                    continue
                print(spec.table_name)
                print(df.head())
                log.info("postgres: table: %s len: %d", spec.table_name, len(df))

                if len(df) > 1:
                    log.info("table: %s has %d rows", spec.table_name, len(df))
                    args = df.to_dict("records")
                    print("args")
                    print(args[:3])
                    fields = list(args[0].keys())
                    log.debug("postgres %s fields: %s", spec.table_name, fields)
                    ins_sql = dc.compose_insert(fields, table_name=spec.table_name)
                    try:
                        await self.conn.executemany(
                            ins_sql, [list(d.values()) for d in args]
                        )
                    except apg.exceptions.UniqueViolationError:
                        # this can happen during testing if writing in < 5 min
                        log.warning("unique violation - could be a restart so continuing")
                    continue

                try:
                    data = df.dropna().squeeze().to_dict()
                    # cull fields we are not storing
                    fields = list(data.keys())
                    log.debug("postgres %s fields: %s", spec.table_name, fields)
                    ins_sql = dc.compose_insert(fields, table_name=spec.table_name)
                    await self.conn.execute(ins_sql, *data.values())
                except apg.exceptions.UniqueViolationError:
                    # this can happen during testing if writing in < 5 min
                    log.warning(
                        "unique violation - could be a fast restart so continuing"
                    )
                except Exception:
                    log.exception(
                        "postgres failed to write %s: %s with %s",
                        spec.table_name,
                        ins_sql,
                        data.values(),
                    )
                log.info("PostgresWriter: %s written", spec.table_name)
            return 0
        except Exception:
            log.exception("PostgresWriter: failed to write: '%s'", spec.table_name)
            raise
        return 0


async def main():
    opt = get_args()
    log.setLevel(level=opt.log_level)
    # consumers = [csv_consume]
    consumers = []
    # if opt.write_json:
    #     consumers.append(json_consume)
    if opt.mq_host:
        mq_writer = MqttConsumer(host=opt.mq_host, topic_template=opt.mq_topic)
        consumers.append(mq_writer.consume)
        log.info(f"added mqtt consumer: {opt.mq_host}")
    if opt.host:
        pg_writer = PostgresConsumer(
            host=opt.host, database=opt.database, user=opt.user, password=opt.password
        )
        consumers.append(pg_writer.write)
        log.info(f"added db consumer: {opt.host}")

    conv = AioConveyor(
        produce=partial(produce, postcode=opt.postcode),
        consumers=consumers,
        loop_interval=opt.loop_interval,
        loop_offset=opt.loop_offset,
    )
    conv.start()
    log.info("main: thread started")
    while conv.running:
        log.debug("main loop")
        await asyncio.sleep(2)
    log.info("main: conveyor thread no longer running, terminating")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.error("amber exiting")
