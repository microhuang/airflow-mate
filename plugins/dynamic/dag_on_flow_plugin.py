# encoding: utf-8

__author__ = "microhuang"
__version__ = "0.2.0"


import os
from airflow.plugins_manager import AirflowPlugin
from airflow.www.views import Airflow as BaseView
#from airflow.www.views import Airflow, auth
from airflow.www import auth, utils as wwwutils
from airflow.www.decorators import action_logging, gzipped
from airflow.security import permissions
from airflow.utils.db import provide_session
from airflow.configuration import AIRFLOW_CONFIG, conf
from flask_appbuilder import expose
from flask import current_app, Blueprint, Markup, request, jsonify, flash, g, Response, redirect, url_for, before_render_template


dag_on_flow_bp = Blueprint(
    "dag_on_flow_bp",
    __name__,
    #url_prefix=dcmp_settings.BASE_URL,
    url_prefix=conf.get('dag_creation_manager', 'base_url'),
    template_folder="templates",
    static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    #static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), '')
    #static_url_path="assets",
    #root_path="/aafds"
)

@dag_on_flow_bp.before_app_request
def before_request():
    #app = current_app._get_current_object()
    #if dcmp_settings.DAG_CREATION_MANAGER_AIRFLOW_TEMPLATES:
    if conf.get('dag_creation_manager', 'dag_creation_manager_airflow_templates'):
        import jinja2
        current_app.jinja_loader = jinja2.ChoiceLoader([
            #jinja2.FileSystemLoader('/var/opt/bigdata/airflow-dag-creation-manager-plugin.git/plugins/dcmp/templates'),
            jinja2.FileSystemLoader(conf.get('dag_creation_manager', 'dag_creation_manager_airflow_templates')),
            #jinja2.FileSystemLoader('/var/opt/bigdata/airflow.git/airflow/www/templates'),
            #jinja2.FileSystemLoader(dcmp_settings.DAG_CREATION_MANAGER_AIRFLOW_TEMPLATES+'/bpmn-dag'),
            current_app.jinja_loader
        ])
        #current_app.config['PLUGIN_BASE_URL'] = dcmp_settings.BASE_URL
        current_app.config['PLUGIN_BASE_URL'] = conf.get('dag_creation_manager', 'base_url')


class DagOnFlowView(BaseView):
    default_view = "graph_diagram"
    #route_base = dcmp_settings.BASE_URL
    route_base = conf.get('dag_creation_manager', 'base_url')

    @expose('/graph_diagram')
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_LOG),
        ]
    )
    @gzipped
    @action_logging
    @provide_session
    def graph_diagram(self, session=None):
        from airflow.models import Connection
        #from airflow.api_connexion.endpoints import connection_endpoint
        #conns = connection_endpoint.get_connections(session, 99999)
        #conns = [c.get('connection_id') for c in conns.get('connections',[]) if c.get('conn_type','')=='ssh']
        #return self.graph_edit(session)
        query = session.query(Connection).filter(Connection.conn_type=='ssh')
        conns = [c.conn_id for c in query.all()]
        flows = []
        dags_folder = conf.get('core', 'dags_folder')
        for flow in os.listdir(dags_folder):
            if flow.endswith('.bpmn'):
                flows.append(flow[:-5])
        return self.render_template(
            #'dcmp/bpmn.html',
            'dcmp/graph_layout.html',
            #app_base=dcmp_settings.BASE_URL,
            app_base=conf.get('dag_creation_manager', 'base_url'),
            conns=conns,
            flows=flows
            )

    @expose('/bpmn_list', methods=["GET"])
    @provide_session
    def bpmn_list(self, session=None):
        ls = os.listdir(os.path.join(conf.get('core','dags_folder')))
        ls = tuple( {'name':l} for l in ls if l.endswith('.bpmn') )
        return jsonify({"code": 200, "data": ls})

    @expose('/bpmn_xml', methods=["GET"])
    @provide_session
    def bpmn_xml(self, session=None):
        dag_id = request.values.get('dag_id')
        if dag_id[-5:]=='.bpmn':
            dag_id = dag_id[0:-5]
        import os
        with open(os.path.join(conf.get('core','dags_folder'), dag_id+'.bpmn'), 'r') as f:
            return f.read()
        pass



@dag_on_flow_bp.route('/bpmn_save', methods=["POST"])
#@expose('/bpmn_save', methods=["POST"])
@provide_session
def bpmn_save(session=None):
        dag_id = request.values.get('dag_id')
        data = request.values.get('data')
        if data:
          import urllib
          import os
          #from xml.dom.minidom import parse
          from xml.dom.minidom import parseString
          try:
            data = urllib.parse.unquote(data)
            domTree = parseString(data)
            rootNode = domTree.documentElement
            processNode = rootNode.getElementsByTagName('bpmn2:process')
            if not processNode:
                processNode = rootNode.getElementsByTagName('process')
                if not processNode:
                    raise Exception('data error')
                else:
                    processNode = processNode[0]
            else:
                processNode = processNode[0]
            if not dag_id:
              dag_id =  processNode.getAttribute('id')
              dag_name = processNode.getAttribute('name')
              if dag_name:
                  dag_id = dag_name
            if not dag_id:
                return jsonify({"code": 500, "detail": "data error", })
            #if os.path.isfile(os.path.join(dcmp_settings.AIRFLOW_DAGS_FOLDER, dag_id+'.bpmn')):
            #    return jsonify({"code": 500, "detail": "dag already exists", })
            #with open(os.path.join(dcmp_settings.AIRFLOW_DAGS_FOLDER, dag_id+'.bpmn'), 'w') as f:
            with open(os.path.join(conf.get('core','dags_folder'), dag_id+'.bpmn'), 'w') as f:
                f.write(data)
                return jsonify({"code": 200, "detail": "done", })
          except Exception as err:
              print(err)
              return jsonify({"code": 501, "detail": "data error", })
        else:
            return jsonify({"code": 502, "detail": "parameter error", })
        return jsonify({"code": 503, "detail": "data error", })


class DagOnFlowPlugin(AirflowPlugin):
#class DagOnFlowPlugin():
    dagOnFlowView = DagOnFlowView()

    dag_manager_view_1 = {
    #"name": "DAGs Manage",
    "name": "-",
    #"category": dcmp_settings.DAG_CREATION_MANAGER_MENU,
    "category": 'Admin',
    "view": dagOnFlowView,
    #"href": url_for('Airflow.graph', dag_id="example_dag")
    #"href": "/ext/graph_edit?dag_id=example_dag"
    }

    dag_manager_view = {
    #"name": "DAGs Manage",
    "name": "GRAPH",
    #"category": dcmp_settings.DAG_CREATION_MANAGER_MENU,
    "category": 'Admin',
    "view": dagOnFlowView,
    #"href": url_for('Airflow.graph', dag_id="example_dag")
    #"href": "/ext/graph_edit?dag_id=example_dag"
    }

    '''
    # hook airflow/www/templates
    if False and dcmp_settings.DAG_CREATION_MANAGER_AIRFLOW_TEMPLATES:
        import jinja2
        current_app.jinja_loader = jinja2.ChoiceLoader([
            #jinja2.FileSystemLoader('/var/opt/bigdata/airflow-dag-creation-manager-plugin.git/plugins/dcmp/templates'),
            jinja2.FileSystemLoader(dcmp_settings.DAG_CREATION_MANAGER_AIRFLOW_TEMPLATES),
            #jinja2.FileSystemLoader('/var/opt/bigdata/airflow.git/airflow/www/templates'),
            current_app.jinja_loader
        ])
        current_app.config['PLUGIN_BASE_URL'] = dcmp_settings.BASE_URL
    '''
    name = "dag_on_flow"
    operators = []
    flask_blueprints = [dag_on_flow_bp]
    hooks = []
    executors = []
    menu_links = []
    appbuilder_views = [dag_manager_view_1, dag_manager_view]
    #appbuilder_views = [dag_manager_view, user_manager_view]
    #appbuilder_menu_items = [appbuilder_mitem]


