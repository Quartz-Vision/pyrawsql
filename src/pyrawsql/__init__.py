from typing import Any, Callable, Literal

from sqlalchemy import BindParameter, bindparam
from sqlalchemy.sql._typing import _TypeEngineArgument
from sqlalchemy.sql.base import _NoArg


class QueryContext:
    """
    Query context to keep track of aliases and bindparams for raw SQL queries.
    """
    def __init__(self):
        """
        Initialize the context for building SQL queries.
        Usable for ONLY ONE query.
        """
        self._aliases: set[str] = set()
        self._binds_values: dict[type, list[tuple[Any, str]]] = {}
        self._binds_count: int = 0
        self._binds: dict[str, BindParameter] = {}

    def bindparam(
        self,
        value: Any,
        type_: _TypeEngineArgument | None = None,
        unique: bool = False,
        required: bool | Literal[_NoArg.NO_ARG] = _NoArg.NO_ARG,
        quote: bool | None = None,
        callable_: Callable[[], Any] | None = None,
        expanding: bool = False,
        isoutparam: bool = False,
        literal_execute: bool = False,
    ) -> str:
        """
        Add a bindparam to the context.
        If a bind is already present (the same value with `is` operator),
        create a new bind with a unique name.

        For args explanation, see: https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.bindparam

        Usage::

            ctx = QueryContext()
            requester_id_bind = ctx.bindparam(user_id)
            query = text(f"select * from users where id = :{requester_id_bind}")
            query.bindparams(*ctx.get_bindparams())

        This is useful to easily keep track of all bindparams used in the query and its subqueries,
        if they were generated somewhere else::

            def has_followers(ctx: QueryContext, user_id: UUID) -> str:
                bind = ctx.bindparam(user_id)
                return f"exists(... where user_id = :{bind})"

            my_id_bind = ctx.bindparam(user_id)
            query = f"select *, {has_followers(ctx, other_user_id)} as ... from users where id = :{my_id_bind}"

            # Here my_id_bind will be different from the bind inside has_followers

        Returns:
            str: The unique or reused name of the bindparam
        """
        value_type = type(value)
        if value_type not in self._binds_values:
            type_group = self._binds_values[value_type] = []
        else:
            type_group = self._binds_values[value_type]
            for bind_value, bind_key in type_group:
                if bind_value is value:
                    return bind_key

        key = f"b{self._binds_count}"
        self._binds_count += 1
        type_group.append((value, key))

        self._binds[key] = bindparam(
            key=key,
            value=value,
            type_=type_,
            unique=unique,
            required=required,
            quote=quote,
            callable_=callable_,
            expanding=expanding,
            isoutparam=isoutparam,
            literal_execute=literal_execute,
        )

        return key

    def get_bindparams(self) -> list[BindParameter]:
        """
        Get all bindparams from the context.
        They are ready to be used in `TextClause.bindparams(...)`.

        Usage::

            id = ctx.bindparam(user_id)
            query = text(f"select * from users where id = :{id}")
            query.bindparams(*ctx.get_bindparams())
        """
        return list(self._binds.values())

    def alias(self, name: str) -> str:
        """
        Create a unique alias for any SQL clause element.

        Usage::

            ctx = QueryContext()
            video_post = ctx.new_alias("vp")
            query = f"select * from video_post {video_post} where {video_post}.user_id = ..."

        If you need the same alias somewhere, just pass it there::

            def calc_age(alias: str):
                return f"(now() - {alias}.birthday)"

            users = ctx.alias("u")
            f"select * from users {users} where {calc_age(users)} > interval '18 years'"

        This is useful to avoid collisions with aliases in subqueries.

        Returns:
            str: New ALWAYS unique name for the alias.
        """
        if name in self._aliases:
            name = f"{name}_{len(self._aliases)}"
        self._aliases.add(name)
        return name