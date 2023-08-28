from __future__ import annotations

import json
import os
from typing import Any, MutableMapping, MutableSequence

import aiopg
import pkg_resources
import psycopg2.extras
from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Target
from streamflow.core.persistence import DependencyType
from streamflow.core.utils import get_date_from_ns
from streamflow.core.workflow import Port, Status, Step, Token
from streamflow.persistence.sqlite import CachedDatabase


class PostgreSQLConnectionPool:
    def __init__(
        self,
        dbname: str,
        hostname: str,
        username: str,
        password: str,
        timeout: int,
        maxsize: int = 10,
    ):
        self.dbname: str = dbname
        self.hostname: str = hostname
        self.username: str = username
        self.password: str = password
        self.timeout: int = timeout
        self.maxsize: int = maxsize
        self._pool: aiopg.Pool | None = None

    async def __aenter__(self):
        if not self._pool:
            self._pool = await aiopg.create_pool(
                database=self.dbname,
                user=self.username,
                password=self.password,
                host=self.hostname,
                timeout=self.timeout,
                maxsize=self.maxsize,
            )
            schema_path = pkg_resources.resource_filename(
                __name__, os.path.join("schemas", "postgresql.sql")
            )
            with open(schema_path) as f:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        await cursor.execute(f.read())
        return self._pool

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def close(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None


class PostgreSQLDatabase(CachedDatabase):
    def __init__(
        self,
        context: StreamFlowContext,
        dbname: str,
        hostname: str,
        username: str,
        password: str,
        timeout: int = 20,
        maxConnections: int = 10,
    ):
        super().__init__(context)
        self.pool: PostgreSQLConnectionPool = PostgreSQLConnectionPool(
            dbname=dbname,
            hostname=hostname,
            username=username,
            password=password,
            timeout=timeout,
            maxsize=maxConnections,
        )

    async def close(self):
        await self.pool.close()

    @classmethod
    def get_schema(cls) -> str:
        return pkg_resources.resource_filename(
            __name__, os.path.join("schemas", "postgresql.json")
        )

    async def add_command(self, step_id: int, tag: str, cmd: str) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO command(step, tag, cmd) "
                        "VALUES(%(step)s, %(tag)s, %(cmd)s) "
                        "RETURNING id",
                        {"step": step_id, "tag": tag, "cmd": cmd},
                    )
                    return (await cursor.fetchone())[0]

    async def add_dependency(
        self, step: int, port: int, type: DependencyType, name: str
    ) -> None:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO dependency(step, port, type, name) "
                        "VALUES(%(step)s, %(port)s, %(type)s, %(name)s) "
                        "ON CONFLICT DO NOTHING",
                        {"step": step, "port": port, "type": type.value, "name": name},
                    )

    async def add_deployment(
        self,
        name: str,
        type: str,
        config: str,
        external: bool,
        lazy: bool,
        workdir: str | None,
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO deployment(name, type, config, external, lazy, workdir) "
                        "VALUES (%(name)s, %(type)s, %(config)s, %(external)s, %(lazy)s, %(workdir)s) "
                        "RETURNING id",
                        {
                            "name": name,
                            "type": type,
                            "config": config,
                            "external": external,
                            "lazy": lazy,
                            "workdir": workdir,
                        },
                    )
                    return (await cursor.fetchone())[0]

    async def add_port(
        self, name: str, workflow_id: int, type: type[Port], params: str
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO port(name, workflow, type, params) "
                        "VALUES(%(name)s, %(workflow)s, %(type)s, %(params)s) "
                        "RETURNING id",
                        {
                            "name": name,
                            "workflow": workflow_id,
                            "type": utils.get_class_fullname(type),
                            "params": json.dumps(params),
                        },
                    )
                    return (await cursor.fetchone())[0]

    async def add_provenance(self, inputs: MutableSequence[int], token: int):
        provenance = [{"dependee": i, "depender": token} for i in inputs]
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    for prov in provenance:
                        await cursor.execute(
                            "INSERT INTO provenance(dependee, depender) "
                            "VALUES(%(dependee)s, %(depender)s) "
                            "ON CONFLICT DO NOTHING",
                            prov,
                        )

    async def add_step(
        self, name: str, workflow_id: int, status: int, type: type[Step], params: str
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO step(name, workflow, status, type, params) "
                        "VALUES(%(name)s, %(workflow)s, %(status)s, %(type)s, %(params)s) "
                        "RETURNING id",
                        {
                            "name": name,
                            "workflow": workflow_id,
                            "status": status,
                            "type": utils.get_class_fullname(type),
                            "params": json.dumps(params),
                        },
                    )
                    return (await cursor.fetchone())[0]

    async def add_target(
        self,
        deployment: int,
        type: type[Target],
        params: str,
        locations: int = 1,
        service: str | None = None,
        workdir: str | None = None,
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO target(params, type, deployment, locations, service, workdir) "
                        "VALUES (%(params)s, %(type)s, %(deployment)s, %(locations)s, %(service)s, %(workdir)s) "
                        "RETURNING id",
                        {
                            "params": json.dumps(params),
                            "type": utils.get_class_fullname(type),
                            "deployment": deployment,
                            "locations": locations,
                            "service": service,
                            "workdir": workdir,
                        },
                    )
                    return (await cursor.fetchone())[0]

    async def add_token(
        self, tag: str, type: type[Token], value: Any, port: int | None = None
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO token(port, type, tag, value) "
                        "VALUES(%(port)s, %(type)s, %(tag)s, %(value)s) "
                        "RETURNING id",
                        {
                            "port": port,
                            "type": utils.get_class_fullname(type),
                            "tag": tag,
                            "value": value,
                        },
                    )
                    return (await cursor.fetchone())[0]

    async def add_workflow(self, name: str, params: str, status: int, type: str) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "INSERT INTO workflow(name, params, status, type) "
                        "VALUES(%(name)s, %(params)s, %(status)s, %(type)s) "
                        "RETURNING id",
                        {
                            "name": name,
                            "params": json.dumps(params),
                            "status": status,
                            "type": type,
                        },
                    )
                    return (await cursor.fetchone())[0]

    async def get_command(self, command_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM command WHERE id = %(id)s", {"id": command_id}
                    )
                    return await cursor.fetchone()

    async def get_commands_by_step(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM command WHERE step = %(id)s", {"id": step_id}
                    )
                    return await cursor.fetchall()

    async def get_dependees(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM provenance WHERE depender = %(id)s",
                        {"id": token_id},
                    )
                    return await cursor.fetchall()

    async def get_dependers(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM provenance WHERE dependee = %(id)s",
                        {"id": token_id},
                    )
                    return await cursor.fetchall()

    async def get_deployment(self, deplyoment_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM deployment WHERE id = %(id)s",
                        {"id": deplyoment_id},
                    )
                    return await cursor.fetchone()

    async def get_input_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM dependency WHERE step = %(step)s AND type = %(type)s",
                        {"step": step_id, "type": DependencyType.INPUT.value},
                    )
                    return await cursor.fetchall()

    async def get_output_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM dependency WHERE step = %(step)s AND type = %(type)s",
                        {"step": step_id, "type": DependencyType.OUTPUT.value},
                    )
                    return await cursor.fetchall()

    async def get_port(self, port_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM port WHERE id = %(id)s", {"id": port_id}
                    )
                    return await cursor.fetchone()

    async def get_port_tokens(self, port_id: int) -> MutableSequence[int]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "SELECT * FROM port WHERE id = %(id)s", {"id": port_id}
                    )
                    return [row[0] for row in await cursor.fetchall()]

    async def get_reports(
        self, workflow: str, last_only: bool = False
    ) -> MutableSequence[MutableSequence[MutableMapping[str, Any]]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    if last_only:
                        await cursor.execute(
                            "SELECT c.id, s.name, c.start_time, c.end_time "
                            "FROM step AS s, command AS c "
                            "WHERE s.id = c.step "
                            "AND s.workflow = (SELECT id FROM workflow WHERE name = %(workflow)s ORDER BY id DESC LIMIT 1)",
                            {"workflow": workflow},
                        )
                        return [[dict(r) for r in await cursor.fetchall()]]
                    else:
                        await cursor.execute(
                            "SELECT s.workflow, c.id, s.name, c.start_time, c.end_time "
                            "FROM step AS s, command AS c "
                            "WHERE s.id = c.step "
                            "AND s.workflow IN (SELECT id FROM workflow WHERE name = %(workflow)s) "
                            "ORDER BY s.workflow DESC",
                            {"workflow": workflow},
                        )
                        result = {}
                        async for row in cursor:
                            result.setdefault(row["workflow"], []).append(
                                {k: row[k] for k in row.keys() if k != "workflow"}
                            )
                        return list(result.values())

    async def get_step(self, step_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM step WHERE id = %(id)s", {"id": step_id}
                    )
                    return await cursor.fetchone()

    async def get_target(self, target_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM target WHERE id = %(id)s", {"id": target_id}
                    )
                    return await cursor.fetchone()

    async def get_token(self, token_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM token WHERE id = %(id)s", {"id": token_id}
                    )
                    return {
                        k: bytearray(v) if isinstance(v, memoryview) else v
                        for k, v in (await cursor.fetchone()).items()
                    }

    async def get_workflow(self, workflow_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM workflow WHERE id = %(id)s", {"id": workflow_id}
                    )
                    return await cursor.fetchone()

    async def get_workflow_ports(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM port WHERE workflow = %(workflow)s",
                        {"workflow": workflow_id},
                    )
                    return await cursor.fetchall()

    async def get_workflow_steps(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM step WHERE workflow = %(workflow)s",
                        {"workflow": workflow_id},
                    )
                    return await cursor.fetchall()

    async def get_workflows_by_name(
        self, workflow_name: str, last_only: bool = False
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    await cursor.execute(
                        "SELECT * FROM workflow WHERE name = %(name)s ORDER BY id desc",
                        {"name": workflow_name},
                    )
                    return (
                        [await cursor.fetchone()]
                        if last_only
                        else await cursor.fetchall()
                    )

    async def get_workflows_list(
        self, name: str | None
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as db:
                if name is not None:
                    return [
                        {
                            "end_time": get_date_from_ns(row["end_time"]),
                            "start_time": get_date_from_ns(row["start_time"]),
                            "status": Status(row["status"]).name,
                            "type": row["type"],
                        }
                        for row in await self.get_workflows_by_name(
                            name, last_only=False
                        )
                    ]
                else:
                    async with db.cursor(
                        cursor_factory=psycopg2.extras.RealDictCursor
                    ) as cursor:
                        await cursor.execute(
                            "SELECT name, type, COUNT(*) AS num FROM workflow GROUP BY name, type ORDER BY name DESC"
                        )
                        return await cursor.fetchall()

    async def update_command(
        self, command_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE command SET {} WHERE id = %(id)s".format(  # nosec
                            ", ".join([f"{k} = %({k})s" for k in updates])
                        ),
                        {**updates, **{"id": command_id}},
                    )
                    self.port_cache.pop(command_id, None)
                    return command_id

    async def update_deployment(
        self, deployment_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE deployment SET {} WHERE id = %(id)s".format(  # nosec
                            ", ".join([f"{k} = %({k})s" for k in updates])
                        ),
                        {**updates, **{"id": deployment_id}},
                    )
                    self.port_cache.pop(deployment_id, None)
                    return deployment_id

    async def update_port(self, port_id: int, updates: MutableMapping[str, Any]) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE port SET {} WHERE id = %(id)s".format(  # nosec
                            ", ".join([f"{k} = %({k})s" for k in updates])
                        ),
                        {**updates, **{"id": port_id}},
                    )
                    self.port_cache.pop(port_id, None)
                    return port_id

    async def update_step(self, step_id: int, updates: MutableMapping[str, Any]) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE step SET {} WHERE id = %(id)s".format(  # nosec
                            ", ".join([f"{k} = %({k})s" for k in updates])
                        ),
                        {**updates, **{"id": step_id}},
                    )
                    self.step_cache.pop(step_id, None)
                    return step_id

    async def update_target(
        self, target_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE target SET {} WHERE id = %(id)s".format(  # nosec
                            ", ".join([f"{k} = %({k})s" for k in updates])
                        ),
                        {**updates, **{"id": target_id}},
                    )
                    self.target_cache.pop(target_id, None)
                    return target_id

    async def update_workflow(
        self, workflow_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as db:
                async with db.cursor() as cursor:
                    await cursor.execute(
                        "UPDATE workflow SET {} WHERE id = %(id)s".format(  # nosec
                            ", ".join([f"{k} = %({k})s" for k in updates])
                        ),
                        {**updates, **{"id": workflow_id}},
                    )
                    self.workflow_cache.pop(workflow_id, None)
                    return workflow_id
