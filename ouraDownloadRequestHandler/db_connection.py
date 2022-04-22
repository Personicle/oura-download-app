import databases
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
import os
import datetime

DATABASE_URL = 'postgresql://{}:{}@{}/{}?sslmode={}'.format(os.environ['DB_CONFIG_USERNAME'], os.environ['DB_CONFIG_PASSWORD'],os.environ['DB_CONFIG_HOST'],os.environ['CREDENTIALS_TABLENAME'], 'prefer')
engine = sqlalchemy.create_engine(
    DATABASE_URL, pool_size=3, max_overflow=0
)
Base = declarative_base(engine)

database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()


users = sqlalchemy.Table(
    os.environ["CREDENTIALS_TABLENAME"],
    metadata,
    sqlalchemy.Column("id", sqlalchemy.INT, primary_key=True),
    sqlalchemy.Column("userId", sqlalchemy.String, primary_key=True,nullable=False),
    sqlalchemy.Column("service", sqlalchemy.String,nullable=False),
    sqlalchemy.Column("access_token", sqlalchemy.String,nullable=False),
    sqlalchemy.Column("expires_in", sqlalchemy.INT,nullable=False),
    sqlalchemy.Column("created_at", sqlalchemy.DATETIME,nullable=False, default=datetime.datetime.utcnow()),
    sqlalchemy.Column("external_user_id", sqlalchemy.String),
    sqlalchemy.Column("refresh_token", sqlalchemy.String),
    sqlalchemy.Column("last_accessed_at", sqlalchemy.DATETIME,nullable=True),
    sqlalchemy.Column("scope", sqlalchemy.String,nullable=True),
    )