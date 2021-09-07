from copy import deepcopy
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)


'''
LOGGING_CONFIG = {
	'version': 1,
	'disable_existing_loggers': False,
	'formatters': {
		'print': {
                    'format': '[%(blue)s%(asctime)s%(reset)s] {{%(blue)s%(filename)s:%(reset)s%(lineno)d}} %(log_color)s%(levelname)s:print:%(reset)s - %(log_color)s%(message)s%(reset)s',
			'class': 'airflow.utils.log.colored_log.CustomTTYColoredFormatter'
		},
		'airflow': {
			'format': '[%(asctime)s] {{%(filename)s:%(lineno)d}} %(levelname)s - %(message)s'
		},
		'airflow_coloured': {
			'format': '[%(blue)s%(asctime)s%(reset)s] {{%(blue)s%(filename)s:%(reset)s%(lineno)d}} %(log_color)s%(levelname)s%(reset)s - %(log_color)s%(message)s%(reset)s',
			'class': 'airflow.utils.log.colored_log.CustomTTYColoredFormatter'
		},
	},
	'filters': {
		'mask_secrets': {
			'()': 'airflow.utils.log.secrets_masker.SecretsMasker'
		}
	},
	'handlers': {
                'print': {
                        'class': 'airflow.utils.log.logging_mixin.RedirectStdHandler',
                        'formatter': 'print',
                        'stream': 'sys.stdout',
                        'filters': ['mask_secrets']
                },
		'console': {
			'class': 'airflow.utils.log.logging_mixin.RedirectStdHandler',
			'formatter': 'airflow_coloured',
			'stream': 'sys.stdout',
			'filters': ['mask_secrets']
		},
		'task': {
			'class': 'airflow.providers.elasticsearch.log.es_task_handler.ElasticsearchTaskHandler',
			'formatter': 'airflow',
			'base_log_folder': '/opt/work/airflow-dag-creation-manager-plugin.git/airflow_home/logs',
			'log_id_template': '{{dag_id}}-{{task_id}}-{{execution_date}}-{{try_number}}',
			'filename_template': '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log',
			'end_of_log_mark': 'end_of_log',
			'host': 'localhost',
			'frontend': '',
			'write_stdout': False,
			'json_format': False,
			'json_fields': 'asctime, filename, lineno, levelname, message',
			'filters': ['mask_secrets']
		},
		'processor': {
			'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
			'formatter': 'airflow',
			'base_log_folder': '/opt/work/airflow-dag-creation-manager-plugin.git/airflow_home/logs/scheduler',
			'filename_template': '{{ filename }}.log',
			'filters': ['mask_secrets']
		}
	},
	'loggers': {
                'print': {
                    'handlers': ['print'],
                    'level': 'DEBUG',
                    'filters': ['mask_secrets'],
		    'propagate': False
                },
		'airflow.processor': {
			'handlers': ['processor'],
			'level': 'INFO',
			'propagate': False
		},
		'airflow.task': {
			'handlers': ['task'],
			'level': 'INFO',
			'propagate': False,
			'filters': ['mask_secrets']
		},
		'flask_appbuilder': {
			'handler': ['console'],
			'level': 'WARNING',
			'propagate': True
		},
	},
	'root': {
		'handlers': ['console'],
		'level': 'INFO',
		'filters': ['mask_secrets']
	}
}
'''

# bootstrap print -> debug
#LOGGING_CONFIG['root']['level'] = 'DEBUG'
#LOGGING_CONFIG['root']['handlers'] = ['print']

LOGGING_CONFIG['formatters']['print'] = {
                    'format': '[%(blue)s%(asctime)s%(reset)s] {{%(blue)s%(pathname)s:%(reset)s%(lineno)d}} %(log_color)s%(levelname)s:print:%(reset)s - \n%(log_color)s%(message)s%(reset)s',
                        'class': 'airflow.utils.log.colored_log.CustomTTYColoredFormatter'
                }

LOGGING_CONFIG['handlers']['print'] = {
                        'class': 'airflow.utils.log.logging_mixin.RedirectStdHandler',
                        'formatter': 'print',
                        'stream': 'sys.stdout',
                        'filters': ['mask_secrets']
                }

LOGGING_CONFIG['loggers']['print'] = {
                    'handlers': ['print'],
                    'level': 'DEBUG',
                    'filters': ['mask_secrets'],
                    'propagate': False
                }

LOGGING_CONFIG['formatters']['airflow_coloured']['format'] = '[%(blue)s%(asctime)s%(reset)s] {{%(blue)s%(pathname)s:%(reset)s%(lineno)d}} %(log_color)s%(levelname)s%(reset)s - %(log_color)s%(message)s%(reset)s'

LOGGING_CONFIG['formatters']['airflow']['format'] = '[%(asctime)s] {{%(pathname)s:%(lineno)d}} %(levelname)s - %(message)s'


LOGGING_CONFIG['handlers']['processor']['formatter'] = 'print'
# print('dddddddddddddddddd', LOGGING_CONFIG['handlers']['processor'])

SQLALCHEMY_ECHO = True


