import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine

data_path_cursos = "2022_cursos_historico.csv"
data_path_examenes = "examenes.csv"
data_path_requisitos = "requisitos.csv"
data_path_reglas = "reglas.csv"
data_path_areas = "areas.csv"
data_path_programas = "programas.csv"
data_path_departamentos = "departamentos.csv"

df_cursos = pd.read_csv(data_path_cursos, sep=';')
df_examenes = pd.read_csv(data_path_examenes, sep=';')
df_requisitos = pd.read_csv(data_path_requisitos, sep=';')
df_reglas = pd.read_csv(data_path_reglas,sep=';')
df_areas = pd.read_csv(data_path_areas, sep=';')
df_programas = pd.read_csv(data_path_programas, sep=';')
df_departamentos = pd.read_csv(data_path_departamentos, sep=';')

df_copy = df_cursos.copy()
departamentos = (df_departamentos.loc[:,'nombre']).values

df_copy['NOMBRE'] = df_copy['NOMBRE'].apply(lambda x: x.capitalize())
df_copy = df_copy.drop(['NIVEL'], axis=1)
df_copy.columns = ['sigla', 'codigo','departamento', 'nombre', 'creditos']
df_copy['es_epsilon'] = False
df_copy['es_tipo_e'] = False

df_copy = df_copy.drop_duplicates(['sigla','codigo'], keep='last')

df_clean = df_copy[df_copy['sigla'].isin(departamentos)]
df_clean = df_clean[df_clean['departamento'].isin(departamentos)]

db = create_engine('postgresql://estudiante:estudiante@localhost:5432/pensumconfig')

df_clean.to_sql(
    'curso_entity',
    db,
    if_exists='append',
    index=False
)

df_examenes.to_sql(
    'examen_entity',
    db,
    if_exists='append',
    index=False
)

df_requisitos.to_sql(
    'requisito_entity',
    db,
    if_exists='append',
    index=False
)
df_reglas.to_sql(
    'regla_entity',
    db,
    if_exists='append',
    index=False
)
df_areas.to_sql(
    'area_entity',
    db,
    if_exists='append',
    index=False
)
df_programas.to_sql(
    'programa_entity',
    db,
    if_exists='append',
    index=False
)

df_departamentos.to_sql(
    'departamento_entity',
    db,
    if_exists='append',
    index=False
)