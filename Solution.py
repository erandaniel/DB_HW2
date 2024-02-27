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

def _result_to_owner_obj(result: Connector.ResultSet) -> Owner:
    pass


def _result_to_customer_obj(result: Connector.ResultSet) -> Customer:
    pass


def _result_to_apartment_obj(result: Connector.ResultSet) -> Apartment:
    pass


# ---------------------------------- CRUD API: ----------------------------------


def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    try:
        query = f"INSERT INTO Owner(id, name) VALUES({owner.get_owner_id()}, {owner.get_owner_name()})"
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def get_owner(owner_id: int) -> Owner:
    _query = f"SELECT * FROM Owner WHERE id={owner_id}"

    conn = None
    try:
        conn = Connector.DBConnector()
        _, result = conn.execute(_query)
    except DatabaseException:
        return Owner.bad_owner()
    finally:
        conn.close()

    if not result:
        return Owner.bad_owner()

    return Owner(owner_id=result[0]['id'], owner_name=result[0]['owner_name'])


def delete_owner(owner_id: int) -> ReturnValue:
    conn = None
    try:
        query = f"DELETE FROM Owner WHERE id={owner_id}"
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query)
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()

    if not rows_effected:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def add_apartment(apartment: Apartment) -> ReturnValue:
    conn = None
    try:
        # TODO: implement query after DB is ready
        query = f"INSERT INTO Apartment(id, name) VALUES()"
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def get_apartment(apartment_id: int) -> Apartment:
    _query = f"SELECT * FROM Apartment WHERE id={apartment_id}"

    conn = None
    try:
        conn = Connector.DBConnector()
        _, result = conn.execute(_query)
    except DatabaseException:
        return Owner.bad_owner()
    finally:
        conn.close()

    if not result:
        return Apartment.bad_apartment()

    return Owner(owner_id=result[0]['id'], owner_name=result[0]['owner_name'])


def delete_apartment(apartment_id: int) -> ReturnValue:
    conn = None
    try:
        query = f"DELETE FROM Apartment WHERE id={apartment_id}"
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query)
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()

    if not rows_effected:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    try:
        # TODO: implement query after DB is ready
        query = f"INSERT INTO Customer(id, name) VALUES({customer.get_customer_id()}, {customer.get_customer_name()})"
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query)
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()

    return ReturnValue.OK


def _get(query):
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(query)
    except DatabaseException:
        return -1, Owner.bad_owner()
    finally:
        conn.close()

    return rows_effected, result


_ERROR_CODE = -1


def _delete(query):
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(query)
    except DatabaseException.CHECK_VIOLATION:
        return ReturnValue.BAD_PARAMS
    except DatabaseException:
        return ReturnValue.ERROR
    finally:
        conn.close()

    if not rows_effected:
        return ReturnValue.NOT_EXISTS

    return rows_effected, _


def get_customer(customer_id: int) -> Customer:
    _query = f"SELECT * FROM Customer WHERE id={customer_id}"
    rows_effected, result = _get(_query)
    if rows_effected == _ERROR_CODE:
        return result
    return Customer(customer_id=result[0]['id'], customer_name=result[0]['name'])


def delete_customer(customer_id: int) -> ReturnValue:
    query = f"DELETE FROM Customer WHERE id={customer_id}"
    rows_effected, result = _delete(query)
    if rows_effected == _ERROR_CODE:
        return result
    return ReturnValue.OK


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    # TODO: implement
    pass


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    # TODO: implement
    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    # TODO: implement
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def owner_doesnt_own_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    # TODO: implement
    pass


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
    # TODO: implement
    pass


def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass


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

ALL_TABLES = ['Owner', 'Customer', 'Apartment']


def create_tables():
    conn = Connector.DBConnector()

    try:
        conn.execute("CREATE TABLE Owner("
                     "id INTEGER PRIMARY KEY,"
                     " name TEXT NOT NULL)")

        conn.execute("CREATE TABLE Customer("
                     "id INTEGER PRIMARY KEY,"
                     " name TEXT NOT NULL)")

        conn.execute(
            "CREATE TABLE Apartment("
            " id INTEGER,"
            " address TEXT NOT NULL,"
            " city TEXT NOT NULL,"
            " country TEXT NOT NULL,"
            " size int NOT NULL,"
            " PRIMARY KEY(city, address))")

    except DatabaseException:
        pass
    finally:
        conn.close()


def clear_tables():
    conn = Connector.DBConnector()
    try:
        for table in ALL_TABLES:
            query = f"DELETE FROM {table}"
            conn.execute(query)
    except DatabaseException:
        pass
    finally:
        conn.close()


def drop_tables():
    conn = Connector.DBConnector()
    try:
        for table in ALL_TABLES:
            query = f"DROP TABLE {table}"
            conn.execute(query)
    except DatabaseException:
        pass
    finally:
        conn.close()
