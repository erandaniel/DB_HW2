from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# TODO: another update (need to check if all the python checking (id < 0) are fine if not then >0 for all ids should be added to the CREATE TABLES

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

    class Rev:
        TABLE_NAME = 'Reviews'
        cid = 'customer_id'
        hid = 'apartment_id'
        review_date = 'review_date'
        rating = 'rating'
        review_text = 'review_text'

    class Res:
        TABLE_NAME = 'Reservations'
        cid = 'customer_id'
        hid = 'apartment_id'
        start_date = 'start_date'
        end_date = 'end_date'
        total_price = 'total_price'

    class RevView:
        TABLE_NAME = 'ReviewsView'
        house_id = 'apartment_id'
        owner_id = 'owner_id'
        rating = 'rating'


def _get(query):
    _conn = None
    try:
        _conn = Connector.DBConnector()
        _rows_effected, result = _conn.execute(query)
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        raise _Ex(ReturnValue.BAD_PARAMS)
    except DatabaseException.UNIQUE_VIOLATION:
        raise _Ex(ReturnValue.ALREADY_EXISTS)
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        raise _Ex(ReturnValue.NOT_EXISTS)
    except DatabaseException:
        raise _Ex(ReturnValue.ERROR)
    finally:
        try:
            _conn.close()
        except Exception:
            raise _Ex(ReturnValue.ERROR)

    return _rows_effected, result


def _insert(query):
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
    except Exception as e:
        raise e
    finally:
        try:
            _conn.close()
        except Exception:
            raise _Ex(ReturnValue.ERROR)

    return _rows_effected, _


def _update(_query):
    rows_updated, _ = _insert(_query)
    if not rows_updated:
        raise _Ex(ReturnValue.NOT_EXISTS)
    return rows_updated, _


def _delete(query):
    _conn = None
    try:
        _conn = Connector.DBConnector()
        rows_effected, _ = _conn.execute(query)
    except (DatabaseException.CHECK_VIOLATION, DatabaseException.NOT_NULL_VIOLATION):
        raise _Ex(ReturnValue.BAD_PARAMS)
    except DatabaseException:
        raise _Ex(ReturnValue.ERROR)
    finally:
        try:
            _conn.close()
        except Exception:
            raise _Ex(ReturnValue.ERROR)

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
        id=result[M.A.id],
        address=result[M.A.address],
        city=result[M.A.city],
        country=result[M.A.country],
        size=result[M.A.size]
    )


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
    if owner_id <= 0:
        return ReturnValue.BAD_PARAMS

    _query = sql.SQL(f"DELETE FROM {M.O.TABLE_NAME} WHERE {M.O.id}={{ID}}").format(
        ID=sql.Literal(owner_id),
    )
    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def add_apartment(apartment: Apartment) -> ReturnValue:
    if not apartment.get_id() or apartment.get_id() <= 0 or apartment.get_size() <= 0:
        return ReturnValue.BAD_PARAMS

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

    return _result_to_apartment_obj(result[0])


def delete_apartment(apartment_id: int) -> ReturnValue:
    if apartment_id <= 0:
        return ReturnValue.BAD_PARAMS

    _query = sql.SQL(f"DELETE FROM {M.A.TABLE_NAME} WHERE {M.A.id}={{ID}}").format(
        ID=sql.Literal(apartment_id)
    )

    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def add_customer(customer: Customer) -> ReturnValue:
    if not customer.get_customer_id() or customer.get_customer_id() <= 0:
        return ReturnValue.BAD_PARAMS

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
    if customer_id <= 0:
        return ReturnValue.BAD_PARAMS

    _query = sql.SQL(f"DELETE FROM {M.C.TABLE_NAME} WHERE {M.C.id}={{ID}}").format(
        ID=sql.Literal(customer_id),
    )
    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    if owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS

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


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    if owner_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS

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
        f"SELECT * FROM {M.OwnedBy.TABLE_NAME} "
        f"LEFT OUTER JOIN {M.A.TABLE_NAME} ON "
        f"{M.OwnedBy.house_id}={M.A.id} "
        f"WHERE {M.OwnedBy.owner_id}={{ID}} "
    ).format(ID=sql.Literal(owner_id),)

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code
    if not rows_effected:
        return []

    return [_result_to_apartment_obj(r) for r in result if result]


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


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    if customer_id <= 0 or apartment_id <= 0 or total_price <=0:
        return ReturnValue.BAD_PARAMS

    _query = sql.SQL(f"INSERT INTO {M.Res.TABLE_NAME} "
                    f"SELECT {{{M.Res.cid}}}, {{{M.Res.hid}}}, {{{M.Res.start_date}}}, {{{M.Res.end_date}}}, {{{M.Res.total_price}}} "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {M.Res.TABLE_NAME} AS Res WHERE {{{M.Res.hid}}} = Res.{M.Res.hid} AND "
                    f"({{{M.Res.start_date}}}, {{{M.Res.end_date}}}) OVERLAPS (Res.{M.Res.start_date}, Res.{M.Res.end_date}))").format(
        customer_id=sql.Literal(customer_id),
        apartment_id=sql.Literal(apartment_id),
        start_date=sql.Literal(start_date),
        end_date=sql.Literal(end_date),
        total_price=sql.Literal(total_price))
    try:
        num_of_rows_changed, _ = _insert(_query)
        if num_of_rows_changed == 0: return ReturnValue.BAD_PARAMS
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    if customer_id <= 0 or apartment_id <= 0:
        return ReturnValue.BAD_PARAMS

    _query = sql.SQL(
        f"DELETE FROM {M.Res.TABLE_NAME} WHERE {M.Res.cid}={{CID}} AND {M.Res.hid}={{HID}} AND {M.Res.start_date}={{START_DATE}}"
    ).format(
        CID=sql.Literal(customer_id),
        HID=sql.Literal(apartment_id),
        START_DATE=sql.Literal(start_date),
    )

    try:
        _delete(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    if customer_id <= 0 or apartment_id <= 0 or rating < 1 or rating > 10:
        return ReturnValue.BAD_PARAMS

    _query = sql.SQL(
        f"INSERT INTO {M.Rev.TABLE_NAME} "
        f"SELECT {{cid}}, {{hid}}, {{DATE}}, {{RATING}}, {{TEXT}} "
        f"WHERE EXISTS (SELECT 1 FROM {M.Res.TABLE_NAME} AS Res "
        f"WHERE {M.Rev.hid} = {{hid}} "
        f"AND Res.{M.Res.end_date} <= {{DATE}} "
        f"AND {M.Rev.cid} = {{cid}} )"
    ).format(
        cid=sql.Literal(customer_id),
        hid=sql.Literal(apartment_id),
        DATE=sql.Literal(review_date),
        RATING=sql.Literal(rating),
        TEXT=sql.Literal(review_text)
    )

    try:
        _rows_effected, _ = _insert(_query)
    except _Ex as e:
        return e.error_code

    if not _rows_effected:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def customer_updated_review(customer_id: int, apartment_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:

    if customer_id <= 0 or apartment_id <= 0 or new_rating < 1 or new_rating > 10:
        return ReturnValue.BAD_PARAMS

    _query = sql.SQL(
        f"UPDATE {M.Rev.TABLE_NAME} "
        f" SET "
        f" {M.Rev.rating}={{RATING}},"
        f" {M.Rev.review_text}={{TEXT}},"
        f" {M.Rev.review_date}={{DATE}}"  # TODO: do we need to update time or override? and constrain update date > date 
        f" WHERE {M.Rev.cid}={{CID}} AND {M.Rev.hid}={{HID}} AND {M.Rev.review_date} <= {{DATE}}"
    ).format(
        CID=sql.Literal(customer_id),
        HID=sql.Literal(apartment_id),
        DATE=sql.Literal(update_date),
        RATING=sql.Literal(new_rating),
        TEXT=sql.Literal(new_text)
    )
    try:
        _update(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def reservations_per_owner() -> List[Tuple[str, int]]:
    _query = sql.SQL(
        f"SELECT {M.O.TABLE_NAME}.{M.O.name}, {M.O.TABLE_NAME}.{M.O.id}, COUNT ({M.Res.TABLE_NAME}.{M.Res.total_price}) as res_count"

        f" FROM {M.O.TABLE_NAME} "
        
        f" LEFT OUTER JOIN {M.OwnedBy.TABLE_NAME} "
        f" ON {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.owner_id} = {M.O.TABLE_NAME}.{M.O.id} "

        f" LEFT OUTER JOIN {M.Res.TABLE_NAME} "
        f" ON {M.Res.TABLE_NAME}.{M.Res.hid} = {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.house_id} "
        

        f" GROUP BY {M.O.TABLE_NAME}.{M.O.id}, {M.O.TABLE_NAME}.{M.O.name}"
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    return [(r[M.C.name], r['res_count']) for r in result]


def get_top_customer() -> Customer:
    _query = sql.SQL(
        f"SELECT {M.C.id}, {M.C.name}, COUNT ({M.C.id})"

        f" FROM {M.Res.TABLE_NAME} "
        f" LEFT OUTER JOIN {M.C.TABLE_NAME} "
        f" ON {M.Res.TABLE_NAME}.{M.Res.cid} = {M.C.TABLE_NAME}.{M.C.id} "
        f" GROUP BY {M.C.TABLE_NAME}.{M.C.id}"
        f" ORDER BY count DESC, {M.C.id} "
        f" LIMIT 1"
    )

    # TODO: can we use limit here or do we need to do double select?

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    return _result_to_customer_obj(result)


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    _query = sql.SQL(
        f"SELECT {M.Res.hid}, (SUM({M.Res.total_price})/12)*0.15 as profit"
        f" FROM {M.Res.TABLE_NAME}"
        f" WHERE EXTRACT(YEAR FROM {M.Res.end_date}) = {{YEAR}}"
        f" GROUP BY {M.Res.hid}").format(
        YEAR=sql.Literal(year),
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    return [(r[M.Res.hid], r['profit']) for r in result]


def get_all_location_owners() -> List[Owner]:
    # TODO: make this query look nicer
    _query = sql.SQL(
        f"""
            SELECT owner_id as OwnerId,  name as Name FROM 
        (
        SELECT owner_id, name, COUNT(owner_id) as a FROM
        (SELECT DISTINCT city, country  FROM public.apartment GROUP BY city, country) AS A
        LEFT OUTER JOIN apartment
        ON apartment.city = A.city
        LEFT OUTER JOIN Ownedby
        ON id = Ownedby.apartment_id
        LEFT OUTER JOIN owner 
        ON owner_id = owner.ownerId
        GROUP BY owner_id, name
        )
        WHERE a = (SELECT COUNT(*) FROM (
        SELECT DISTINCT city, country  FROM public.apartment GROUP BY city, country 
            ))
        """
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code
    return [_result_to_owner_obj([r]) for r in result]


def best_value_for_money() -> Apartment:
    # TODO: make this query look nicer
    _query = sql.SQL(
        f"""
        SELECT * FROM (
        SELECT reservations.apartment_id, AVG(rating)/(SUM(total_price)/SUM(end_date-start_date)) as value
        	FROM public.reservations
        	LEFT OUTER JOIN public.reviews 
        	ON reservations.apartment_id = reviews.apartment_id
        	GROUP BY reservations.apartment_id
        	ORDER BY value 
        	LIMIT 1
        ) as A
        LEFT OUTER JOIN public.apartment
        ON A.apartment_id = apartment.id
        """
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    return _result_to_apartment_obj(result[0])


def get_apartment_rating(apartment_id: int) -> float:
    if apartment_id <= 0:
        return 0

    # must use view (the same view as get_owner_rating and get_apartment_rating)


    _query = sql.SQL(
        f"SELECT AVG({M.RevView.rating}) as avg_rating"
        f" FROM {M.RevView.TABLE_NAME}"
        f" WHERE {M.RevView.house_id} = {{HID}}"
        f" GROUP BY {M.RevView.house_id}").format(
        HID=sql.Literal(apartment_id)
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    if not rows_effected:
        return 0

    return result[0]["avg_rating"]


def get_owner_rating(owner_id: int) -> float:
    # must use view (the same view as get_owner_rating and get_apartment_rating)

    if owner_id <= 0:
        return 0

    _query = sql.SQL(
        f" SELECT COALESCE(AVG(average),0) as avg_owner_rating "
        f" FROM ViewAptRating "
        f" WHERE EXISTS ("
        f" SELECT 1 FROM {M.OwnedBy.TABLE_NAME} WHERE "
        f" {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.house_id}=ViewAptRating.{M.OwnedBy.house_id} "
        f" AND {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.owner_id}={{OID}}"
        f" )").format(
        OID=sql.Literal(owner_id)
    )
        
    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    if not rows_effected:
        return 0

    return result[0]["avg_owner_rating"]


# ---------------------------------- ADVANCED API: ----------------------------------


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement almost done
    temp_query = f"""
SELECT   apartment_id, AVG(rating),  AVG_DIST_RATIO , SUM(rating)*AVG_DIST_RATIO AS EXPECTED FROM
(SELECT C.customer_id, public.reviews.customer_id AS REVIWER, AVG_DIST_RATIO, apartment_id, rating FROM
(SELECT customer_id, AVG(DIST_RATIO) AS AVG_DIST_RATIO FROM
(SELECT DISTINCT *, rating/RATING_AVG_EXCLUSING AS DIST_RATIO FROM   
(SELECT 
	apartment_id,
	COUNT(rating) as RATERS_EXCLUDING, 
	SUM(rating) AS RATING_SUM_EXCLUSING,
	AVG(rating) AS RATING_AVG_EXCLUSING
	FROM public.reviews
	WHERE customer_id != 2
	GROUP BY apartment_id 
) AS A
	LEFT JOIN
	(SELECT customer_id, rating FROM public.reviews)
	ON apartment_id = apartment_id
    WHERE RATING_SUM_EXCLUSING != 0
)
WHERE customer_id = 2
GROUP BY customer_id
) AS C , public.reviews
WHERE C.customer_id != public.reviews.customer_id
) AS D
GROUP BY apartment_id, AVG_DIST_RATIO
"""
    pass


# ---------------------------------- 5.1 Basic Database Functions ----------------------------------


ALL_TABLES = [M.A.TABLE_NAME, M.C.TABLE_NAME, M.O.TABLE_NAME, M.OwnedBy.TABLE_NAME,
              M.Rev.TABLE_NAME, M.Res.TABLE_NAME, M.RevView.TABLE_NAME]


def create_tables():
    quries = [
        f"CREATE TABLE {M.O.TABLE_NAME}(" #Owner
        f"{M.O.id} INTEGER PRIMARY KEY CHECK ({M.O.id} > 0),"
        f" {M.O.name} TEXT NOT NULL)"
        ,

        f"CREATE TABLE {M.C.TABLE_NAME}("
        f"{M.C.id} INTEGER PRIMARY KEY,"
        f" {M.C.name} TEXT NOT NULL)",

        f"CREATE TABLE {M.A.TABLE_NAME}("
        f" {M.A.id} INTEGER UNIQUE,"
        f" {M.A.address} TEXT NOT NULL,"
        f" {M.A.city} TEXT NOT NULL,"
        f" {M.A.country} TEXT NOT NULL,"
        f" {M.A.size} int NOT NULL,"
        f" PRIMARY KEY({M.A.city}, {M.A.address})"
        # f" PRIMARY KEY({M.A.id})"
        f")",

        f"CREATE TABLE {M.OwnedBy.TABLE_NAME}("
        f"{M.OwnedBy.owner_id} INTEGER NOT NULL,"
        f" {M.OwnedBy.house_id} INTEGER NOT NULL,"
        f" PRIMARY KEY({M.OwnedBy.house_id}),"
        f"FOREIGN KEY ({M.OwnedBy.owner_id}) REFERENCES {M.O.TABLE_NAME}({M.O.id}),"
        f"FOREIGN KEY ({M.OwnedBy.house_id}) REFERENCES {M.A.TABLE_NAME}({M.A.id})"
        f")",

        f"CREATE TABLE {M.Res.TABLE_NAME}("
        f" {M.Res.cid} INTEGER NOT NULL,"
        f" {M.Res.hid} INTEGER NOT NULL,"
        f" {M.Res.start_date} DATE NOT NULL,"
        f" {M.Res.end_date} DATE NOT NULL,"
        f" {M.Res.total_price} FLOAT NOT NULL,"
        f" PRIMARY KEY({M.Res.hid}, {M.Res.start_date}, {M.Res.end_date}),"
        f" FOREIGN KEY ({M.Res.cid}) REFERENCES {M.C.TABLE_NAME}({M.C.id}),"
        f" FOREIGN KEY ({M.Res.hid}) REFERENCES {M.A.TABLE_NAME}({M.A.id})"
        f")",

        f"CREATE TABLE {M.Rev.TABLE_NAME}("
        f" {M.Rev.cid} INTEGER NOT NULL,"
        f" {M.Rev.hid} INTEGER NOT NULL,"
        f" {M.Rev.review_date} DATE NOT NULL,"
        f" {M.Rev.rating} INTEGER NOT NULL,"
        f" {M.Rev.review_text} TEXT NOT NULL,"
        f" PRIMARY KEY({M.Rev.cid}, {M.Rev.hid}),"
        f" FOREIGN KEY ({M.Rev.cid}) REFERENCES {M.C.TABLE_NAME}({M.C.id}),"
        f" FOREIGN KEY ({M.Rev.hid}) REFERENCES {M.A.TABLE_NAME}({M.A.id})"
        f")",

        f"CREATE VIEW {M.RevView.TABLE_NAME} "
        f" AS"
        f" SELECT {M.Rev.TABLE_NAME}.{M.Rev.hid}, {M.OwnedBy.owner_id}, {M.Rev.rating}"
        f" FROM {M.Rev.TABLE_NAME}"
        f" LEFT OUTER JOIN {M.OwnedBy.TABLE_NAME}"
        f" ON {M.Rev.TABLE_NAME}.{M.Rev.hid} = {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.house_id} ",

        f"CREATE VIEW ViewAptRating "
        f" AS"
        f" SELECT {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.owner_id}, {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.house_id}, AVG(COALESCE(rating, 0)) AS average"
        f" FROM {M.OwnedBy.TABLE_NAME}"
        f" LEFT OUTER JOIN {M.Rev.TABLE_NAME}"
        f" ON {M.Rev.TABLE_NAME}.{M.Rev.hid} = {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.house_id} "
        f" GROUP BY {M.OwnedBy.TABLE_NAME}.{M.OwnedBy.house_id}",

        # TODO: add contains for all tables
        # TODO: make sure the review date is after reservation have ended
    ]

    for q in quries:
        try:
            conn = Connector.DBConnector()
            conn.execute(
                sql.SQL(
                    q
                )
            )
        except Exception as e:
            print(e)
        finally:
            conn.close()


def clear_tables():
    for table in ALL_TABLES:
        conn = Connector.DBConnector()
        query = f"DELETE FROM {table}"
        try:
            conn.execute(query)
        except Exception as e:
            print(e)
            pass
        finally:
            conn.close()


def drop_tables():
    for table in ALL_TABLES:
        conn = Connector.DBConnector()
        query = f"DROP TABLE {table} CASCADE"
        try:
            conn.execute(query)
        except Exception as e:
            print(e)
            pass
        finally:
            conn.close()


pass
