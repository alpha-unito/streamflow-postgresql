from __future__ import annotations

import json
import os
from typing import Any, MutableMapping, MutableSequence

import asyncpg
import pkg_resources
from streamflow.core import utils
from streamflow.core.context import StreamFlowContext
from streamflow.core.deployment import Target
from streamflow.core.persistence import DependencyType
from streamflow.core.utils import get_date_from_ns
from streamflow.core.workflow import Port, Status, Step, Token
from streamflow.persistence.base import CachedDatabase


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
        self._pool: asyncpg.Pool | None = None

    async def __aenter__(self):
        if not self._pool:
            self._pool = await asyncpg.create_pool(
                database=self.dbname,
                user=self.username,
                password=self.password,
                host=self.hostname,
                timeout=self.timeout,
                max_size=self.maxsize,
            )
            schema_path = pkg_resources.resource_filename(
                __name__, os.path.join("schemas", "postgresql.sql")
            )
            with open(schema_path) as f:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute(f.read())
        return self._pool

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def close(self):
        if self._pool:
            await self._pool.close()
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
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchval(
                        "INSERT INTO command(step, tag, cmd) "
                        "VALUES($1, $2, $3) "
                        "RETURNING id",
                        step_id,
                        tag,
                        cmd.encode("utf-8"),
                    )

    async def add_dependency(
        self, step: int, port: int, type: DependencyType, name: str
    ) -> None:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO dependency(step, port, type, name) "
                        "VALUES($1, $2, $3, $4) "
                        "ON CONFLICT DO NOTHING",
                        step,
                        port,
                        type.value,
                        name,
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
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchval(
                        "INSERT INTO deployment(name, type, config, external, lazy, workdir) "
                        "VALUES ($1, $2, $3, $4, $5, $6) "
                        "RETURNING id",
                        name,
                        type,
                        config,
                        external,
                        lazy,
                        workdir,
                    )

    async def add_port(
        self, name: str, workflow_id: int, type: type[Port], params: str
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchval(
                        "INSERT INTO port(name, workflow, type, params) "
                        "VALUES($1, $2, $3, $4) "
                        "RETURNING id",
                        name,
                        workflow_id,
                        utils.get_class_fullname(type),
                        json.dumps(params),
                    )

    async def add_provenance(self, inputs: MutableSequence[int], token: int):
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(
                        "INSERT INTO provenance(dependee, depender) "
                        "VALUES($1, $2) "
                        "ON CONFLICT DO NOTHING",
                        [(i, token) for i in inputs],
                    )

    async def add_step(
        self, name: str, workflow_id: int, status: int, type: type[Step], params: str
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchval(
                        "INSERT INTO step(name, workflow, status, type, params) "
                        "VALUES($1, $2, $3, $4, $5) "
                        "RETURNING id",
                        name,
                        workflow_id,
                        status,
                        utils.get_class_fullname(type),
                        json.dumps(params),
                    )

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
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchval(
                        "INSERT INTO target(params, type, deployment, locations, service, workdir) "
                        "VALUES ($1, $2, $3, $4, $5, $6) "
                        "RETURNING id",
                        json.dumps(params),
                        utils.get_class_fullname(type),
                        deployment,
                        locations,
                        service,
                        workdir,
                    )

    async def add_token(
        self, tag: str, type: type[Token], value: Any, port: int | None = None
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchval(
                        "INSERT INTO token(port, type, tag, value) "
                        "VALUES($1, $2, $3, $4) "
                        "RETURNING id",
                        port,
                        utils.get_class_fullname(type),
                        tag,
                        bytearray(value, "utf-8"),
                    )

    async def add_workflow(self, name: str, params: str, status: int, type: str) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    return await conn.fetchval(
                        "INSERT INTO workflow(name, params, status, type) "
                        "VALUES($1, $2, $3, $4) "
                        "RETURNING id",
                        name,
                        json.dumps(params),
                        status,
                        type,
                    )

    async def get_command(self, command_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM command WHERE id = $1", command_id
                )
                return {
                    k: bytearray(v) if isinstance(v, memoryview) else v
                    for k, v in row.items()
                }

    async def get_commands_by_step(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT * FROM command WHERE step = $1", step_id
                )
                return [
                    {
                        k: bytearray(v) if isinstance(v, memoryview) else v
                        for k, v in row.items()
                    }
                    for row in rows
                ]

    async def get_dependees(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT * FROM provenance WHERE depender = $1",
                    token_id,
                )

    async def get_dependers(
        self, token_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT * FROM provenance WHERE dependee = $1",
                    token_id,
                )

    async def get_deployment(self, deplyoment_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM deployment WHERE id = $1",
                    deplyoment_id,
                )

    async def get_input_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT * FROM dependency WHERE step = $1 AND type = $2",
                    step_id,
                    DependencyType.INPUT.value,
                )

    async def get_output_ports(
        self, step_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT * FROM dependency WHERE step = $1 AND type = $2",
                    step_id,
                    DependencyType.OUTPUT.value,
                )

    async def get_port(self, port_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetchrow("SELECT * FROM port WHERE id = $1", port_id)

    async def get_port_tokens(self, port_id: int) -> MutableSequence[int]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM port WHERE id = $1", port_id)
                return [row["id"] for row in rows]

    async def get_reports(
        self, workflow: str, last_only: bool = False
    ) -> MutableSequence[MutableSequence[MutableMapping[str, Any]]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                if last_only:
                    rows = await conn.fetch(
                        "SELECT c.id, s.name, c.start_time, c.end_time "
                        "FROM step AS s, command AS c "
                        "WHERE s.id = c.step "
                        "AND s.workflow = ("
                        "SELECT id FROM workflow "
                        "WHERE name = $1 "
                        "ORDER BY id DESC LIMIT 1)",
                        workflow,
                    )
                    return [[dict(r) for r in rows]]
                else:
                    async with conn.transaction():
                        cursor = conn.cursor(
                            "SELECT s.workflow, c.id, s.name, c.start_time, c.end_time "
                            "FROM step AS s, command AS c "
                            "WHERE s.id = c.step "
                            "AND s.workflow IN (SELECT id FROM workflow WHERE name = $1) "
                            "ORDER BY s.workflow DESC",
                            workflow,
                        )
                        result = {}
                        async for row in cursor:
                            result.setdefault(row["workflow"], []).append(
                                {k: row[k] for k in row.keys() if k != "workflow"}
                            )
                        return list(result.values())

    async def get_step(self, step_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetchrow("SELECT * FROM step WHERE id = $1", step_id)

    async def get_target(self, target_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM target WHERE id = $1", target_id
                )

    async def get_token(self, token_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT * FROM token WHERE id = $1", token_id)
                return {
                    k: bytearray(v) if isinstance(v, memoryview) else v
                    for k, v in row.items()
                }

    async def get_workflow(self, workflow_id: int) -> MutableMapping[str, Any]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM workflow WHERE id = $1", workflow_id
                )

    async def get_workflow_ports(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT * FROM port WHERE workflow = $1",
                    workflow_id,
                )

    async def get_workflow_steps(
        self, workflow_id: int
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT * FROM step WHERE workflow = $1",
                    workflow_id,
                )

    async def get_workflows_by_name(
        self, workflow_name: str, last_only: bool = False
    ) -> MutableSequence[MutableMapping[str, Any]]:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                query = "SELECT * FROM workflow WHERE name = $1 ORDER BY id desc"
                return (
                    [conn.fetchrow(query, workflow_name)]
                    if last_only
                    else conn.fetch(query, workflow_name)
                )

    async def get_workflows_list(
        self, name: str | None
    ) -> MutableSequence[MutableMapping[str, Any]]:
        if name is not None:
            return [
                {
                    "end_time": get_date_from_ns(row["end_time"]),
                    "start_time": get_date_from_ns(row["start_time"]),
                    "status": Status(row["status"]).name,
                    "type": row["type"],
                }
                for row in await self.get_workflows_by_name(name, last_only=False)
            ]
        else:
            async with self.pool as pool:
                async with pool.acquire() as conn:
                    return await conn.fetch(
                        "SELECT name, type, COUNT(*) AS num "
                        "FROM workflow GROUP BY name, type "
                        "ORDER BY name DESC"
                    )

    async def update_command(
        self, command_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE command SET {} WHERE id = $1".format(  # nosec
                            ", ".join([f"{k} = ${i+2}" for i, k in enumerate(updates)])
                        ),
                        command_id,
                        *updates.values(),
                    )
                    self.port_cache.pop(command_id, None)
                    return command_id

    async def update_deployment(
        self, deployment_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE deployment SET {} WHERE id = $1".format(  # nosec
                            ", ".join([f"{k} = ${i+2}" for i, k in enumerate(updates)])
                        ),
                        deployment_id,
                        *updates.values(),
                    )
                    self.port_cache.pop(deployment_id, None)
                    return deployment_id

    async def update_port(self, port_id: int, updates: MutableMapping[str, Any]) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE port SET {} WHERE id = $1".format(  # nosec
                            ", ".join([f"{k} = ${i+2}" for i, k in enumerate(updates)])
                        ),
                        port_id,
                        *updates.values(),
                    )
                    self.port_cache.pop(port_id, None)
                    return port_id

    async def update_step(self, step_id: int, updates: MutableMapping[str, Any]) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE step SET {} WHERE id = $1".format(  # nosec
                            ", ".join([f"{k} = ${i+2}" for i, k in enumerate(updates)])
                        ),
                        step_id,
                        *updates.values(),
                    )
                    self.step_cache.pop(step_id, None)
                    return step_id

    async def update_target(
        self, target_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE target SET {} WHERE id = $1".format(  # nosec
                            ", ".join([f"{k} = ${i+2}" for i, k in enumerate(updates)])
                        ),
                        target_id,
                        *updates.values(),
                    )
                    self.target_cache.pop(target_id, None)
                    return target_id

    async def update_workflow(
        self, workflow_id: int, updates: MutableMapping[str, Any]
    ) -> int:
        async with self.pool as pool:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE workflow SET {} WHERE id = $1".format(  # nosec
                            ", ".join([f"{k} = ${i+2}" for i, k in enumerate(updates)])
                        ),
                        workflow_id,
                        *updates.values(),
                    )
                    self.workflow_cache.pop(workflow_id, None)
                    return workflow_id
