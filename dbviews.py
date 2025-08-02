import sqlalchemy as sa
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table


class CreateView(DDLElement):
	def __init__(self, name=None, select=None, schema='public', metadata=None):
		self.name = name
		self.schema = schema
		self.select = select
		sa.event.listen(metadata, 'after_create', self)
		sa.event.listen(metadata, 'before_drop', DropView(name, schema))


class CreateView2(DDLElement):
	def __init__(self, name, selectable):
		self.name = name
		self.selectable = selectable


class DropView(DDLElement):
	def __init__(self, name, schema):
		self.name = name
		self.schema = schema


class DropView2(DDLElement):
	def __init__(self, name):
		self.name = name


@compiler.compiles(CreateView)
def createGen(element, compiler, **kwargs):
	return f'CREATE OR REPLACE VIEW {element.schema}."{element.name}" AS {compiler.sql_compiler.process(element.select, literal_binds=True)}'


@compiler.compiles(DropView)
def dropGen(element, compiler, **kw):
	return f'DROP VIEW {element.schema}."{element.name}"'


@compiler.compiles(CreateView)
def _create_view(element, compiler, **kw):
	return f"CREATE VIEW {element.name} AS {compiler.sql_compiler.process(element.selectable, literal_binds=True)}"


@compiler.compiles(DropView)
def _drop_view(element, compiler, **kw):
	return f"DROP VIEW {element.name}"
