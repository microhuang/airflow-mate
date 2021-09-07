from functools import wraps
from airflow.www import auth
from airflow.www.auth import T
from airflow.security import permissions
from typing import Callable, Optional, Sequence, Tuple, TypeVar, cast
from flask import current_app, flash, redirect, request, url_for

auth_has_access = auth.has_access

def wrapper_has_access(_permissions: Optional[Sequence[Tuple[str, str]]] = None) -> Callable[[T], T]:
    # permissions.RESOURCE_DAG -> permissions.RESOURCE_DAG_ROLE
    '''
    if (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG) in _permissions and not (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_ROLE) in _permissions:
        _permissions.append((permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_ROLE))
        _permissions.remove((permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG))
        ret = auth_has_access(_permissions)
        if not ret:
            _permissions.append((permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG))
            _permissions.remove((permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_ROLE))
            return auth_has_access(_permissions)
        return ret
    return auth_has_access(_permissions)
    '''
    def requires_access_decorator(func: T):
        @wraps(func)
        def decorated(*args, **kwargs):
            appbuilder = current_app.appbuilder
            # TODO: xxx
            if (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG) in _permissions and not (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_ROLE) in _permissions:
                _permissions.append((permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_ROLE))
                _permissions.remove((permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG))
                if appbuilder.sm.check_authorization(_permissions, request.args.get('dag_id', None)):
                    return func(*args, **kwargs)
                _permissions.append((permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG))
                _permissions.remove((permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_ROLE))
            if appbuilder.sm.check_authorization(_permissions, request.args.get('dag_id', None)):
                return func(*args, **kwargs)
            else:
                access_denied = "plugin: Access is Denied"
                flash(access_denied, "danger")
            return redirect(
                url_for(
                    appbuilder.sm.auth_view.__class__.__name__ + ".login",
                    next=request.url,
                )
            )

        return cast(T, decorated)

    return requires_access_decorator

#auth.has_access = wrapper_has_access



