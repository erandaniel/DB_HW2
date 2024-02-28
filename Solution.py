from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# date exaples
# TODO: remove
# format: date(year: int, month: int, day: int)
# example: 24/12/2023
# d = date(2023, 12, 24)
# Basic getters:
# print(d.year) # prints “2023”
# print(d.month) # prints “12”
# print(d.day) # prints “24”
# print(d.strftime('%Y-%m-%d')) # prints “2023-12-24”
# print(d.strftime('%A %B %d')) # prints “Sunday December 24”


# ---------------------------------- private functions: ----------------------------------
class _Ex(Exception):
    def __init__(self, value):
        self.error_code = value


class M:
    class O:
        TABLE_NAME = 'Owner'
        id = 'OwnerID'
        name = "Name"

    class A:
        TABLE_NAME = 'Apartment'
        id = "ID"
        address = 'Address'
        city = 'City'
        country = 'Country'
        size = 'Size'

    class C:
        TABLE_NAME = 'Customer'
        id = 'id'
        name = 'name'

    class OwnedBy:
        TABLE_NAME = 'OwnedBy'
        owner_id = 'owner_id'
        house_id = 'apartment_id'


def _get(query):
    try:
        _conn = Connector.DBConnector()
        rows_effected, result = _conn.execute(query)
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        raise _Ex(ReturnValue.BAD_PARAMS)
    except DatabaseException.UNIQUE_VIOLATION:
        raise _Ex(ReturnValue.ALREADY_EXISTS)
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        raise _Ex(ReturnValue.NOT_EXISTS)
    except DatabaseException:
        raise _Ex(ReturnValue.ERROR)
    finally:
        _conn.close()

    return rows_effected, result


def _insert(query):
    # query = sql.SQL(query)
    _conn = None
    try:
        _conn = Connector.DBConnector()
        _rows_effected, _ = _conn.execute(query)
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        raise _Ex(ReturnValue.BAD_PARAMS)
    except DatabaseException.UNIQUE_VIOLATION:
        raise _Ex(ReturnValue.ALREADY_EXISTS)
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        raise _Ex(ReturnValue.NOT_EXISTS)
    except DatabaseException:
        raise _Ex(ReturnValue.ERROR)
    finally:
        _conn.close()


def _delete(query):
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query)
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        raise _Ex(ReturnValue.BAD_PARAMS)
    except DatabaseException:
        raise _Ex(ReturnValue.ERROR)
    finally:
        conn.close()
    if not rows_effected:
        raise _Ex(ReturnValue.NOT_EXISTS)

    return rows_effected, _


def _result_to_owner_obj(result: Connector.ResultSet) -> Owner:
    return Owner(
        owner_id=result[0][M.O.id],
        owner_name=result[0][M.O.name],
    )


def _result_to_customer_obj(result: Connector.ResultSet) -> Customer:
    return Customer(
        customer_id=result[0][M.C.id],
        customer_name=result[0][M.C.name],
    )


def _result_to_apartment_obj(result: Connector.ResultSet) -> Apartment:
    return Apartment(
        id=result[0][M.A.id],
        address=result[0][M.A.address],
        city=result[0][M.A.city],
        country=result[0][M.A.country],
        size=result[0][M.A.size]
    )


# ---------------------------------- WORKING_ON_NOW ----------------------------------


def get_apartment_owner(apartment_id: int) -> Owner:
    _query = sql.SQL(
        f"SELECT * FROM {M.OwnedBy.TABLE_NAME} LEFT OUTER JOIN {M.O.TABLE_NAME} ON {M.OwnedBy.owner_id} = {M.O.id} WHERE {M.OwnedBy.house_id}={{ID}}").format(
        ID=sql.Literal(apartment_id),
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code
    if not rows_effected:
        return Owner.bad_owner()

    return _result_to_owner_obj(result)


# ---------------------------------- CRUD API: ----------------------------------


def add_owner(owner: Owner) -> ReturnValue:
    _query = sql.SQL(f"INSERT INTO {M.O.TABLE_NAME}({M.O.id}, {M.O.name}) VALUES({{ID}}, {{Name}})").format(
        ID=sql.Literal(owner.get_owner_id()),
        Name=sql.Literal(owner.get_owner_name())
    )
    try:
        _insert(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def get_owner(owner_id: int) -> Owner:
    _query = sql.SQL(f"SELECT * FROM {M.O.TABLE_NAME} WHERE {M.O.id}={{ID}}").format(
        ID=sql.Literal(owner_id),
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    if not rows_effected:
        return Owner.bad_owner()

    return _result_to_owner_obj(result)


def delete_owner(owner_id: int) -> ReturnValue:
    # TODO: when getting None need to return BAD_PARAMS and not NOT_EXISTS
    _query = sql.SQL(f"DELETE FROM {M.O.TABLE_NAME} WHERE {M.O.id}={{ID}}").format(
        ID=sql.Literal(owner_id),
    )
    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def add_apartment(apartment: Apartment) -> ReturnValue:
    _query = sql.SQL(
        f"INSERT INTO {M.A.TABLE_NAME}({M.A.id}, {M.A.address}, {M.A.city}, {M.A.country}, {M.A.size}) "
        f"VALUES({{ID}}, {{Address}}, {{City}}, {{Country}}, {{Size}})").format(
        ID=sql.Literal(apartment.get_id()),
        Address=sql.Literal(apartment.get_address()),
        City=sql.Literal(apartment.get_city()),
        Country=sql.Literal(apartment.get_country()),
        Size=sql.Literal(apartment.get_size())
    )
    try:
        _insert(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def get_apartment(apartment_id: int) -> Apartment:
    _query = sql.SQL(f"SELECT * FROM {M.A.TABLE_NAME} WHERE {M.A.id}={{ID}}").format(
        ID=sql.Literal(apartment_id)
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return Apartment.bad_apartment()
    if not rows_effected:
        return Apartment.bad_apartment()

    return _result_to_apartment_obj(result)


def delete_apartment(apartment_id: int) -> ReturnValue:
    _query = sql.SQL(f"DELETE FROM {M.A.TABLE_NAME} WHERE {M.A.id}={{ID}}").format(
        ID=sql.Literal(apartment_id)
    )

    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def add_customer(customer: Customer) -> ReturnValue:
    _query = sql.SQL(f"INSERT INTO {M.C.TABLE_NAME}({M.C.id}, {M.C.name}) VALUES({{ID}}, {{Name}})").format(
        ID=sql.Literal(customer.get_customer_id()),
        Name=sql.Literal(customer.get_customer_name())
    )
    try:
        _insert(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def get_customer(customer_id: int) -> Customer:
    _query = sql.SQL(f"SELECT * FROM {M.C.TABLE_NAME} WHERE {M.C.id}={{ID}}").format(
        ID=sql.Literal(customer_id),
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    if not rows_effected:
        return Customer.bad_customer()

    return _result_to_customer_obj(result)


def delete_customer(customer_id: int) -> ReturnValue:
    # TODO: when getting None need to return BAD_PARAMS and not NOT_EXISTS
    _query = sql.SQL(f"DELETE FROM {M.C.TABLE_NAME} WHERE {M.C.id}={{ID}}").format(
        ID=sql.Literal(customer_id),
    )
    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    _query = sql.SQL(
        f"INSERT INTO {M.OwnedBy.TABLE_NAME}({M.OwnedBy.owner_id}, {M.OwnedBy.house_id}) VALUES({{OWNER_ID}}, {{HOUSE_ID}})").format(
        OWNER_ID=sql.Literal(owner_id),
        HOUSE_ID=sql.Literal(apartment_id)
    )
    try:
        _insert(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def owner_doesnt_own_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: when getting None need to return BAD_PARAMS and not NOT_EXISTS
    _query = sql.SQL(
        f"DELETE FROM {M.OwnedBy.TABLE_NAME} WHERE {M.OwnedBy.owner_id}={{OWNER_ID}} AND {M.OwnedBy.house_id}={{HOUSE_ID}}").format(
        OWNER_ID=sql.Literal(owner_id),
        HOUSE_ID=sql.Literal(apartment_id)
    )
    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    _query = sql.SQL(
        f"SELECT * FROM {M.OwnedBy.TABLE_NAME} LEFT OUTER JOIN {M.O.TABLE_NAME} ON {M.OwnedBy.owner_id} = {M.O.id} WHERE {M.OwnedBy.owner_id}={{ID}}").format(
        ID=sql.Literal(owner_id),
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code
    if not rows_effected:
        return []

    return [_result_to_owner_obj([r]) for r in result]


############# reservation

def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    _query = f"INSERT INTO Reservations(customer_id, apartment_id, start_date, end_date, total_price) VALUES({customer_id}, {apartment_id}, {start_date}, {end_date}, {total_price})"
    try:
        _insert(_query)  # TODO: make sure insert is working here
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    _query = f"DELETE FROM Reservations WHERE customer_id={customer_id} AND apartment_id={apartment_id} AND start_date={start_date}"
    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


############# reviewe

def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    _query = f'INSERT INTO Reviews(customer_id ,apartment_id ,review_date ,rating ,review_text) VALUES({customer_id} ,{apartment_id} ,{review_date} ,{rating} ,{review_text})'
    try:
        _insert(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def customer_updated_review(customer_id: int, apartment_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:
    _query = f'UPDATE Reviews SET review_date = {update_date}, rating = {new_rating}, review_text = {new_text} WHERE customer_id = {customer_id} AND apartment_id = {apartment_id};'
    try:
        _insert(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    # must use view (the same view as get_owner_rating and get_apartment_rating)
    # TODO: implement
    pass


def get_owner_rating(owner_id: int) -> float:
    # must use view (the same view as get_owner_rating and get_apartment_rating)
    # TODO: implement
    pass


def get_top_customer() -> Customer:
    _query = 'SELECT MAX(customer_id), MAX(COUNT(customer_id)) FROM Reservations GROUP BY customer_id'
    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code
    if not result:
        return Owner.bad_owner()
    return _result_to_customer_obj(result)


def reservations_per_owner() -> List[Tuple[str, int]]:
    _query = 'SELECT '
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

OWN_RELATION = 'own'


def get_all_location_owners() -> List[Owner]:
    # get all owners who have a house in city where there is house in
    _query = f'SELECT DISTINCT pid, name FROM {OWN_RELATION}'
    rows_effected, result = _get(_query)
    return [_result_to_owner_obj(r) for r in result]


def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass


# ---------------------------------- 5.1 Basic Database Functions ----------------------------------


ALL_TABLES = [M.A.TABLE_NAME, M.C.TABLE_NAME, M.O.TABLE_NAME, M.OwnedBy.TABLE_NAME]


def create_tables():
    conn = Connector.DBConnector()

    try:

        conn.execute(f"CREATE TABLE {M.O.TABLE_NAME}("
                     f"{M.O.id} INTEGER PRIMARY KEY,"
                     f" {M.O.name} TEXT NOT NULL)")

        conn.execute(f"CREATE TABLE {M.C.TABLE_NAME}("
                     f"{M.C.id} INTEGER PRIMARY KEY,"
                     f" {M.C.name} TEXT NOT NULL)")

        conn.execute(
            f"CREATE TABLE {M.A.TABLE_NAME}("
            f" {M.A.id} INTEGER,"
            f" {M.A.address} TEXT NOT NULL,"
            f" {M.A.city} TEXT NOT NULL,"
            f" {M.A.country} TEXT NOT NULL,"
            f" {M.A.size} int NOT NULL,"
            f" PRIMARY KEY({M.A.id})"
            f")")

        conn.execute(f"CREATE TABLE {M.OwnedBy.TABLE_NAME}("
                     f"{M.OwnedBy.owner_id} INTEGER NOT NULL,"
                     f" {M.OwnedBy.house_id} INTEGER NOT NULL,"
                     f" PRIMARY KEY({M.OwnedBy.owner_id}, {M.OwnedBy.house_id}),"
                     f"FOREIGN KEY ({M.OwnedBy.owner_id}) REFERENCES {M.O.TABLE_NAME}({M.O.id}),"
                     f"FOREIGN KEY ({M.OwnedBy.house_id}) REFERENCES {M.A.TABLE_NAME}({M.A.id})"
                     f")")

        # TODO: make sure positive int
        # TODO: should we save each day as a raw?
        # TODO: what are the keys?
        # conn.execute(
        #     "CREATE TABLE Reservations("
        #     " customer_id INTEGER,"
        #     " apartment_id INTEGER NOT NULL,"
        #     " start_date DATE NOT NULL,"
        #     " end_date DATE NOT NULL,"
        #     " total_price FLOAT NOT NULL,"
        #     " PRIMARY KEY(apartment_id, address))")

    except DatabaseException:
        pass
    finally:
        conn.close()


def clear_tables():
    conn = Connector.DBConnector()
    for table in ALL_TABLES:
        query = f"DELETE FROM {table}"
        try:
            conn.execute(query)
        except Exception:
            pass
    conn.close()


def drop_tables():
    conn = Connector.DBConnector()
    for table in ALL_TABLES:
        query = f"DROP TABLE {table}"
        try:
            conn.execute(query)
        except Exception:
            pass
    conn.close()


pass
