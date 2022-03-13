import unittest

import os
import sys

from dotenv import load_dotenv, find_dotenv


sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.nice_crud import PostgresCrud


load_dotenv(dotenv_path=find_dotenv(raise_error_if_not_found=True))


psql_crud = PostgresCrud(
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT')),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
)
table_name = 'test_table'


def reset(create_table: bool = False, insert_data: bool = False) -> None:
    psql_crud.drop_table(table_name)

    if create_table is True:
        psql_crud.create_table(
            table_name=table_name,
            columns={
                'id': 'serial',
                'name': 'text NOT NULL',
                'family': 'text NOT NULL',
                'age': 'integer NOT NULL',
            },
            unique_keys=['family'],
            primary_key='id',
        )

    if insert_data is True:
        psql_crud.insert_from_dict(
            table_name=table_name,
            data={
                'name': 'john',
                'family': 'doe',
                'age': 20,
            },
        )


class CreateTestTable(unittest.TestCase):
    crud = psql_crud
    table_name = table_name
    primary_key = 'id'
    unique_keys = ['name', 'family']

    def test_via_string(self) -> None:
        try:
            reset()
        except Exception as e:
            print(e)

        try:
            self.crud.create_table(
                table_name=self.table_name,
                columns="id serial, name text NOT NULL, family text NOT NULL, age integer NOT NULL",
                unique_keys=self.unique_keys,
                primary_key=self.primary_key
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_dict(self) -> None:
        try:
            reset()
        except Exception as e:
            print(e)

        try:
            self.crud.create_table(
                table_name=self.table_name,
                columns={
                    'id': 'serial',
                    'name': 'text NOT NULL',
                    'family': 'text NOT NULL',
                    'age': 'integer NOT NULL'
                },
                unique_keys=self.unique_keys,
                primary_key=self.primary_key
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list_of_lists(self) -> None:
        try:
            reset()
        except Exception as e:
            print(e)

        try:
            self.crud.create_table(
                table_name=self.table_name,
                columns=[
                    ['id', 'serial'],
                    ['name', 'text NOT NULL'],
                    ['family', 'text NOT NULL'],
                    ['age', 'integer NOT NULL']
                ],
                unique_keys=self.unique_keys,
                primary_key=self.primary_key
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list_of_dicts(self) -> None:
        try:
            reset()
        except Exception as e:
            print(e)

        try:
            self.crud.create_table(
                table_name=self.table_name,
                columns=[
                    {'id': 'serial'},
                    {'name': 'text NOT NULL'},
                    {'family': 'text NOT NULL'},
                    {'age': 'integer NOT NULL'}
                ],
                unique_keys=self.unique_keys,
                primary_key=self.primary_key
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list_of_tuples(self) -> None:
        try:
            reset()
        except Exception as e:
            print(e)

        try:
            self.crud.create_table(
                table_name=self.table_name,
                columns=[
                    ('id', 'serial'),
                    ('name', 'text NOT NULL'),
                    ('family', 'text NOT NULL'),
                    ('age', 'integer NOT NULL')
                ],
                unique_keys=self.unique_keys,
                primary_key=self.primary_key
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_tuples_of_tuples(self) -> None:
        try:
            reset()
        except Exception as e:
            print(e)

        try:
            self.crud.create_table(
                table_name=self.table_name,
                columns=(
                    ('id', 'serial'),
                    ('name', 'text NOT NULL'),
                    ('family', 'text NOT NULL'),
                    ('age', 'integer NOT NULL')
                ),
                unique_keys=self.unique_keys,
                primary_key=self.primary_key
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_tuples_of_dict(self) -> None:
        try:
            reset()
        except Exception as e:
            print(e)

        try:
            self.crud.create_table(
                table_name=self.table_name,
                columns=(
                    {'id': 'serial'},
                    {'name': 'text NOT NULL'},
                    {'family': 'text NOT NULL'},
                    {'age': 'integer NOT NULL'}
                ),
                unique_keys=self.unique_keys,
                primary_key=self.primary_key
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_tuples_of_lists(self) -> None:
        try:
            reset()
        except Exception as e:
            print(e)

        try:
            self.crud.create_table(
                table_name=self.table_name,
                columns=(
                    ['id', 'serial'],
                    ['name', 'text NOT NULL'],
                    ['family', 'text NOT NULL'],
                    ['age', 'integer NOT NULL']
                ),
                unique_keys=self.unique_keys,
                primary_key=self.primary_key
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)


class InsertTestData(unittest.TestCase):
    crud = psql_crud
    table_name = table_name

    columns = ['name', 'family', 'age']
    values1 = ['john', 'doe', '42']
    values2 = ['Jane', 'doe', '43']

    def test_via_string(self) -> None:
        try:
            reset(create_table=True)
        except Exception as e:
            print(e)

        try:
            self.crud.insert(
                table_name=self.table_name,
                columns='name, family, age',
                values="'john', 'doe', 43",
                on_conflict='do nothing'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_dict(self) -> None:
        try:
            reset(create_table=True)
        except Exception as e:
            print(e)

        try:
            self.crud.insert_from_dict(
                table_name=self.table_name,
                data={
                    'name': 'john',
                    'family': 'doe',
                    'age': 43
                },
                on_conflict='do nothing'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list_of_data(self) -> None:
        try:
            reset(create_table=True)
        except Exception as e:
            print(e)

        try:
            self.crud.insert(
                table_name=self.table_name,
                columns=['name', 'family', 'age'],
                values=["'john'", "'doe'", '43'],
                on_conflict='do nothing'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_tuples_of_data(self) -> None:
        try:
            reset(create_table=True)
        except Exception as e:
            print(e)

        try:
            self.crud.insert(
                table_name=self.table_name,
                columns=('name', 'family', 'age'),
                values=("'john'", "'doe'", '42'),
                on_conflict='do nothing'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)


class SelectTestData(unittest.TestCase):
    crud = psql_crud
    table_name = table_name

    def test_via_string_one(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            result = self.crud.select(
                table_name=self.table_name,
                columns='*',
                condition='name = \'john\'',
                order_by='name',
                limit=1
            )
            is_exception = False

        except Exception as e:
            print(e)
            is_exception = True
            result = None

        self.assertFalse(is_exception)
        self.assertEqual(result, [(1, 'john', 'doe', 20)])

    def test_via_string_two(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            result = self.crud.select(
                table_name=self.table_name,
                columns='name, family, age',
                condition='name = \'john\''
            )
            is_exception = False

        except Exception as e:
            print(e)
            is_exception = True
            result = None

        self.assertFalse(is_exception)
        self.assertEqual(result, [('john', 'doe', 20)])

    def test_via_string_three(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            result = self.crud.select(
                table_name=self.table_name,
                columns='name, family',
                condition='name = \'john\'',
                order_by='name'
            )
            is_exception = False

        except Exception as e:
            print(e)
            is_exception = True
            result = None

        self.assertEqual(result, [('john', 'doe')])
        self.assertFalse(is_exception)

    def test_via_list(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            result = self.crud.select(
                table_name=self.table_name,
                columns=[
                    'name',
                    'family',
                    'age'
                ],
                condition='name = \'john\'',
                order_by='name'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True
            result = None

        self.assertFalse(is_exception)
        self.assertEqual(result, [('john', 'doe', 20)])

    def via_tuple(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            result = self.crud.select(
                table_name=self.table_name,
                columns=(
                    'name',
                    'family',
                    'age',
                ),
                condition='name = \'john\'',
                order_by='name'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True
            result = None

        self.assertFalse(is_exception)
        self.assertEqual(result, [('john', 'doe', 20)])


class UpdateTestData(unittest.TestCase):
    crud = psql_crud
    table_name = table_name

    def test_via_list_and_list(self) -> None:
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.update(
                table_name=self.table_name,
                columns=[
                    'family',
                    'age'
                ],
                values=[
                    'nice',
                    '100'
                ],
                condition='name = \'john\'',
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list_and_tuple(self) -> None:
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.update(
                table_name=self.table_name,
                columns=[
                    'family',
                    'age'
                ],
                values=(
                    'nice',
                    '100'
                ),
                condition='name = \'john\'',
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_tuple_and_list(self) -> None:
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.update(
                table_name=self.table_name,
                columns=(
                    'family',
                    'age'
                ),
                values=[
                    'nice',
                    '100'
                ],
                condition='name = \'john\'',
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def via_tuple_and_tuple(self) -> None:
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.update(
                table_name=self.table_name,
                columns=(
                    'family',
                    'age'
                ),
                values=(
                    'nice',
                    '100'
                ),
                condition='name = \'john\'',
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_dict(self) -> None:
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.update_via_dict(
                table_name=self.table_name,
                data={
                    'family': 'nice',
                    'age': 100
                },
                condition=[
                    'name = \'aaaaaaa\'',
                    'family = \'doe\''
                ],
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)


class DeleteTestData(unittest.TestCase):
    crud = psql_crud
    table_name = table_name

    def test_via_string(self) -> None:

        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.delete(
                table_name=self.table_name,
                condition='name = \'john\'',
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list(self) -> None:
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.delete(
                table_name=self.table_name,
                condition=[
                    "name = 'john'",
                    "age = 50"
                ],
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_tuple(self) -> None:
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.delete(
                table_name=self.table_name,
                condition=(
                    "name = 'john'",
                    "age = 50"
                ),
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)


class DropTestTable(unittest.TestCase):
    crud = psql_crud
    table_name = table_name

    def test_drop(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.drop_table(table_name=self.table_name)
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)


class CreateIndex(unittest.TestCase):
    crud = psql_crud
    table_name = table_name

    def test_via_string(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.create_index(
                table_name=self.table_name,
                columns='name ASC, family ASC',
                unique=True,
                index_name='name_family_index'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.create_index(
                table_name=self.table_name,
                columns=['name ASC', 'family ASC'],
                unique=True,
                index_name='name_family_index'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_tuple(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.create_index(
                table_name=self.table_name,
                columns=('name ASC', 'family ASC'),
                unique=True,
                index_name='name_family_index'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_dict(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.create_index(
                table_name=self.table_name,
                columns={
                    'name': 'ASC',
                    'family': 'ASC'
                },
                unique=True,
                index_name='name_family_index'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list_of_list(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.create_index(
                table_name=self.table_name,
                columns=[
                    ['name', 'ASC'],
                    ['family', 'DESC']
                ],
                unique=True,
                index_name='name_family_index'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_list_of_tuple(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.create_index(
                table_name=self.table_name,
                columns=[
                    ('name', 'ASC NULLS FIRST'),
                    ('family', 'DESC NULLS LAST')
                ],
                unique=True,
                index_name='name_family_index'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)

    def test_via_tuple_of_list(self):
        try:
            reset(create_table=True, insert_data=True)
        except Exception as e:
            print(e)

        try:
            self.crud.create_index(
                table_name=self.table_name,
                columns=(
                    ['name', 'ASC'],
                    ['family', 'DESC']
                ),
                unique=True,
                index_name='name_family_index'
            )
            is_exception = False
        except Exception as e:
            print(e)
            is_exception = True

        self.assertFalse(is_exception)


if __name__ == '__main__':
    unittest.main()
