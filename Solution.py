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


M_O_TABLE_NAME = 'Owner'
M_O_id = 'OwnerID'
M_O_name = "Name"
M_A_TABLE_NAME = 'Apartment'
M_A_id = "ID"
M_A_address = 'Address'
M_A_city = 'City'
M_A_country = 'Country'
M_A_size = 'Size'
M_C_TABLE_NAME = 'Customer'
M_C_id = 'id'
M_C_name = 'name'
M_OwnedBy_TABLE_NAME = 'OwnedBy'
M_OwnedBy_owner_id = 'owner_id'
M_OwnedBy_house_id = 'apartment_id'
M_Rev_TABLE_NAME = 'Reviews'
M_Rev_cid = 'customer_id'
M_Rev_hid = 'apartment_id'
M_Rev_review_date = 'review_date'
M_Rev_rating = 'rating'
M_Rev_review_text = 'review_text'
M_Res_TABLE_NAME = 'Reservations'
M_Res_cid = 'customer_id'
M_Res_hid = 'apartment_id'
M_Res_start_date = 'start_date'
M_Res_end_date = 'end_date'
M_Res_total_price = 'total_price'
M_RevView_TABLE_NAME = 'ReviewsView'
M_RevView_house_id = 'apartment_id'
M_RevView_owner_id = 'owner_id'
M_RevView_rating = 'rating'


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
        owner_id=result[0][M_O_id],
        owner_name=result[0][M_O_name],
    )


def _result_to_customer_obj(result: Connector.ResultSet) -> Customer:
    return Customer(
        customer_id=result[0][M_C_id],
        customer_name=result[0][M_C_name],
    )


def _result_to_apartment_obj(result: Connector.ResultSet) -> Apartment:
    return Apartment(
        id=result[M_A_id],
        address=result[M_A_address],
        city=result[M_A_city],
        country=result[M_A_country],
        size=result[M_A_size]
    )


# ---------------------------------- CRUD API: ----------------------------------


def add_owner(owner: Owner) -> ReturnValue:
    _query = sql.SQL(f"INSERT INTO {M_O_TABLE_NAME}({M_O_id}, {M_O_name}) VALUES({{ID}}, {{Name}})").format(
        ID=sql.Literal(owner.get_owner_id()),
        Name=sql.Literal(owner.get_owner_name())
    )
    try:
        _insert(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def get_owner(owner_id: int) -> Owner:
    _query = sql.SQL(f"SELECT * FROM {M_O_TABLE_NAME} WHERE {M_O_id}={{ID}}").format(
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

    _query = sql.SQL(f"DELETE FROM {M_O_TABLE_NAME} WHERE {M_O_id}={{ID}}").format(
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
        f"INSERT INTO {M_A_TABLE_NAME}({M_A_id}, {M_A_address}, {M_A_city}, {M_A_country}, {M_A_size}) "
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
    _query = sql.SQL(f"SELECT * FROM {M_A_TABLE_NAME} WHERE {M_A_id}={{ID}}").format(
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

    _query = sql.SQL(f"DELETE FROM {M_A_TABLE_NAME} WHERE {M_A_id}={{ID}}").format(
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

    _query = sql.SQL(f"INSERT INTO {M_C_TABLE_NAME}({M_C_id}, {M_C_name}) VALUES({{ID}}, {{Name}})").format(
        ID=sql.Literal(customer.get_customer_id()),
        Name=sql.Literal(customer.get_customer_name())
    )
    try:
        _insert(_query)
    except _Ex as e:
        return e.error_code
    return ReturnValue.OK


def get_customer(customer_id: int) -> Customer:
    _query = sql.SQL(f"SELECT * FROM {M_C_TABLE_NAME} WHERE {M_C_id}={{ID}}").format(
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

    _query = sql.SQL(f"DELETE FROM {M_C_TABLE_NAME} WHERE {M_C_id}={{ID}}").format(
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
        f"INSERT INTO {M_OwnedBy_TABLE_NAME}({M_OwnedBy_owner_id}, {M_OwnedBy_house_id}) VALUES({{OWNER_ID}}, {{HOUSE_ID}})").format(
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
        f"DELETE FROM {M_OwnedBy_TABLE_NAME} WHERE {M_OwnedBy_owner_id}={{OWNER_ID}} AND {M_OwnedBy_house_id}={{HOUSE_ID}}").format(
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
        f"SELECT * FROM {M_OwnedBy_TABLE_NAME} "
        f"LEFT OUTER JOIN {M_A_TABLE_NAME} ON "
        f"{M_OwnedBy_house_id}={M_A_id} "
        f"WHERE {M_OwnedBy_owner_id}={{ID}} "
    ).format(ID=sql.Literal(owner_id), )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code
    if not rows_effected:
        return []

    return [_result_to_apartment_obj(r) for r in result if result]


def get_apartment_owner(apartment_id: int) -> Owner:
    _query = sql.SQL(
        f"SELECT * FROM {M_OwnedBy_TABLE_NAME} LEFT OUTER JOIN {M_O_TABLE_NAME} ON {M_OwnedBy_owner_id} = {M_O_id} WHERE {M_OwnedBy_house_id}={{ID}}").format(
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
    if customer_id <= 0 or apartment_id <= 0 or total_price <= 0:
        return ReturnValue.BAD_PARAMS

    _query = sql.SQL(f"INSERT INTO {M_Res_TABLE_NAME} "
                     f"SELECT {{{M_Res_cid}}}, {{{M_Res_hid}}}, {{{M_Res_start_date}}}, {{{M_Res_end_date}}}, {{{M_Res_total_price}}} "
                     f"WHERE NOT EXISTS (SELECT 1 FROM {M_Res_TABLE_NAME} AS Res WHERE {{{M_Res_hid}}} = Res.{M_Res_hid} AND "
                     f"({{{M_Res_start_date}}}, {{{M_Res_end_date}}}) OVERLAPS (Res.{M_Res_start_date}, Res.{M_Res_end_date}))").format(
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
        f"DELETE FROM {M_Res_TABLE_NAME} WHERE {M_Res_cid}={{CID}} AND {M_Res_hid}={{HID}} AND {M_Res_start_date}={{START_DATE}}"
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
        f"INSERT INTO {M_Rev_TABLE_NAME} "
        f"SELECT {{cid}}, {{hid}}, {{DATE}}, {{RATING}}, {{TEXT}} "
        f"WHERE EXISTS (SELECT 1 FROM {M_Res_TABLE_NAME} AS Res "
        f"WHERE {M_Rev_hid} = {{hid}} "
        f"AND Res.{M_Res_end_date} <= {{DATE}} "
        f"AND {M_Rev_cid} = {{cid}} )"
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
        f"UPDATE {M_Rev_TABLE_NAME} "
        f" SET "
        f" {M_Rev_rating}={{RATING}},"
        f" {M_Rev_review_text}={{TEXT}},"
        f" {M_Rev_review_date}={{DATE}}"  # TODO: do we need to update time or override? and constrain update date > date 
        f" WHERE {M_Rev_cid}={{CID}} AND {M_Rev_hid}={{HID}} AND {M_Rev_review_date} <= {{DATE}}"
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
        f"SELECT {M_O_TABLE_NAME}.{M_O_name}, {M_O_TABLE_NAME}.{M_O_id}, COUNT ({M_Res_TABLE_NAME}.{M_Res_total_price}) as res_count"

        f" FROM {M_O_TABLE_NAME} "

        f" LEFT OUTER JOIN {M_OwnedBy_TABLE_NAME} "
        f" ON {M_OwnedBy_TABLE_NAME}.{M_OwnedBy_owner_id} = {M_O_TABLE_NAME}.{M_O_id} "

        f" LEFT OUTER JOIN {M_Res_TABLE_NAME} "
        f" ON {M_Res_TABLE_NAME}.{M_Res_hid} = {M_OwnedBy_TABLE_NAME}.{M_OwnedBy_house_id} "


        f" GROUP BY {M_O_TABLE_NAME}.{M_O_id}, {M_O_TABLE_NAME}.{M_O_name}"
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    return [(r[M_C_name], r['res_count']) for r in result]


def get_top_customer() -> Customer:
    _query = sql.SQL(
        f"SELECT {M_C_id}, {M_C_name}, COUNT ({M_C_id})"

        f" FROM {M_Res_TABLE_NAME} "
        f" LEFT OUTER JOIN {M_C_TABLE_NAME} "
        f" ON {M_Res_TABLE_NAME}.{M_Res_cid} = {M_C_TABLE_NAME}.{M_C_id} "
        f" GROUP BY {M_C_TABLE_NAME}.{M_C_id}"
        f" ORDER BY count DESC, {M_C_id} "
        f" LIMIT 1"
    )

    # TODO: can we use limit here or do we need to do double select?

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    return _result_to_customer_obj(result)


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # we will add a dummy res with price of 0 for all houses it won't affect apt with res
    _query = sql.SQL(
        f'''
SELECT EXTRACT(MONTH FROM end_date) as month, (SUM(total_price))*0.15 as profit FROM ( 
	
SELECT * FROM Reservations WHERE EXTRACT(YEAR FROM end_date) = {{YEAR}}
UNION
(
SELECT   Reservations.customer_id, 
	     Apartment.ID as apartment_id,
		  Reservations.start_date, 
	      Reservations.end_date, 
		  Reservations.total_price
		  FROM 
		  Apartment 
		 ,
( SELECT  0 AS customer_id,  DATE('{{YEAR}}-01-01') AS start_date,
generate_series(DATE '{{YEAR}}-01-30', DATE '{{YEAR}}-12-30', interval '1 month') AS end_date,
 0 AS total_price) as Reservations
)
	
) GROUP BY EXTRACT(MONTH FROM end_date) 
ORDER BY EXTRACT(MONTH FROM end_date) 
'''
    ).format(
        YEAR=sql.Literal(year),
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    return [(r['month'], r['profit']) for r in result]


def get_all_location_owners() -> List[Owner]:
    # TODO: make this query look nicer
    _query = sql.SQL(
        f"""
            SELECT owner_id as OwnerId,  name as Name FROM 
        (
        SELECT owner_id, name, COUNT(owner_id) as a FROM
        (SELECT DISTINCT city, country  FROM public.apartment GROUP BY city, country) AS A
        LEFT OUTER JOIN apartment
        ON apartment.city = A.city AND apartment.country = A.country
        LEFT OUTER JOIN Ownedby
        ON id = Ownedby.apartment_id
        LEFT OUTER JOIN owner 
        ON owner_id = owner.ownerId
        GROUP BY owner_id, name
        )
        WHERE a = (SELECT COUNT(*) FROM (
        SELECT DISTINCT city, country  FROM public.apartment GROUP BY city, country 
            ))
        ORDER BY ownerid
        """
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code
    return [_result_to_owner_obj([r]) for r in result]


def best_value_for_money() -> Apartment:
    # TODO: rating are counted more than once if someone made more than one reservation
    _query = sql.SQL(f""" SELECT * FROM
 (SELECT * FROM viewaptvalue  WHERE value>= 0 ORDER BY VALUE DESC LIMIT 1) AS BEST
 LEFT JOIN
 Apartment 
 ON Apartment.id = BEST.apartment_id
        """)
    #     f"""
    # SELECT * FROM (
    #    SELECT * FROM (SELECT DISTINCT reservations.apartment_id,
    # 				  AVG(rating) AS RATE,
    # 				  SUM(total_price),
    # 				  SUM(1+end_date-start_date) AS NIGHTS2 ,
    # 				  COALESCE(AVG(rating)/(SUM(total_price)/SUM(1+end_date-start_date)),0) as value
    #     	FROM public.reservations
    #     	LEFT JOIN public.reviews
    #     	ON reservations.apartment_id = reviews.apartment_id AND reservations.customer_id = reviews.customer_id
    #     	GROUP BY reservations.apartment_id
    # 		)
    # 		WHERE value >= 0
    #     ) as A
    #     LEFT OUTER JOIN public.apartment
    #     ON A.apartment_id = apartment.id
    # 	ORDER BY value DESC
    #     LIMIT 1
    #     """

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
        f"SELECT AVG({M_RevView_rating}) as avg_rating"
        f" FROM {M_RevView_TABLE_NAME}"
        f" WHERE {M_RevView_house_id} = {{HID}}"
        f" GROUP BY {M_RevView_house_id}").format(
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
        f" SELECT COALESCE(AVG(average_rating),0) as avg_owner_rating "
        f" FROM ViewAptRating "
        f" WHERE EXISTS ("
        f" SELECT 1 FROM {M_OwnedBy_TABLE_NAME} WHERE "
        f" {M_OwnedBy_TABLE_NAME}.{M_OwnedBy_house_id}=ViewAptRating.{M_OwnedBy_house_id} "
        f" AND {M_OwnedBy_TABLE_NAME}.{M_OwnedBy_owner_id}={{OID}}"
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
    _query = sql.SQL(
        f"""
        SELECT * FROM
    Apartment INNER JOIN 
    (
    SELECT apartment_id, AVG(ratio_rated) FROM
    (
        SELECT *, rating, LEAST(GREATEST(rating*ratio_from_user, 1),10) as ratio_rated, apartment_id as hid FROM
        (
            Reviews
            LEFT JOIN
            (
                SELECT c1, c2, AVG(ratio) as ratio_from_user FROM
                    (
                        SELECT r1.customer_id as c1, r2.customer_id as c2, r1.rating as c1_rate, r2.rating as c2_rate, CAST(r1.rating AS FLOAT) / CAST(r2.rating AS FLOAT) AS ratio , r1.* , r2.* FROM reviews as r1  LEFT OUTER JOIN reviews as r2 On r1.customer_id != r2.customer_id AND r1.apartment_id = r2.apartment_id WHERE r2.customer_id IS NOT NULL AND r1.customer_id = {{CID}} ORDER BY r1.apartment_id
                    )
                GROUP BY c1, c2
            )
            ON c2=customer_id
        )
    )
    WHERE NOT EXISTS (SELECT * FROM Reviews WHERE Reviews.customer_id = {{CID}} AND hid=Reviews.apartment_id)
    GROUP BY apartment_id
    )
    ON id=apartment_id
    ORDER BY id
    """
    ).format(
        CID=sql.Literal(customer_id)
    )

    try:
        rows_effected, result = _get(_query)
    except _Ex as e:
        return e.error_code

    return [(_result_to_apartment_obj(r), float(r['avg'])) for r in result]


# ---------------------------------- 5.1 Basic Database Functions ----------------------------------


ALL_TABLES = [M_Rev_TABLE_NAME, M_Res_TABLE_NAME, M_OwnedBy_TABLE_NAME, M_C_TABLE_NAME, M_A_TABLE_NAME, M_O_TABLE_NAME,
              ]

ALL_VIEWS = ["ViewAptRating", 'ViewPricePerNight', "ViewAptValue", M_RevView_TABLE_NAME]


def create_tables():
    quries = [
        f"CREATE TABLE {M_O_TABLE_NAME}("  # Owner
        f"{M_O_id} INTEGER PRIMARY KEY CHECK ({M_O_id} > 0),"
        f" {M_O_name} TEXT NOT NULL)"
        ,

        f"CREATE TABLE {M_C_TABLE_NAME}("
        f"{M_C_id} INTEGER PRIMARY KEY CHECK ({M_C_id} > 0),"
        f" {M_C_name} TEXT NOT NULL)",

        f"CREATE TABLE {M_A_TABLE_NAME}("
        f" {M_A_id} INTEGER PRIMARY KEY CHECK ({M_A_id} > 0),"
        f" {M_A_address} TEXT NOT NULL,"
        f" {M_A_city} TEXT NOT NULL,"
        f" {M_A_country} TEXT NOT NULL,"
        f" {M_A_size} int NOT NULL,"
        f" UNIQUE({M_A_city}, {M_A_address})"
        f")",

        f"CREATE TABLE {M_OwnedBy_TABLE_NAME}("
        f"{M_OwnedBy_owner_id} INTEGER NOT NULL,"
        f" {M_OwnedBy_house_id} INTEGER NOT NULL,"
        f" PRIMARY KEY({M_OwnedBy_house_id}),"
        f"FOREIGN KEY ({M_OwnedBy_owner_id}) REFERENCES {M_O_TABLE_NAME}({M_O_id} ) ON DELETE CASCADE ,"
        f"FOREIGN KEY ({M_OwnedBy_house_id}) REFERENCES {M_A_TABLE_NAME}({M_A_id} ) ON DELETE CASCADE "
        f")",

        f"CREATE TABLE {M_Res_TABLE_NAME}("
        f" {M_Res_cid} INTEGER NOT NULL,"
        f" {M_Res_hid} INTEGER NOT NULL,"
        f" {M_Res_start_date} DATE NOT NULL,"
        f" {M_Res_end_date} DATE NOT NULL,"
        f" {M_Res_total_price} FLOAT NOT NULL,"
        f" PRIMARY KEY({M_Res_hid}, {M_Res_start_date}, {M_Res_end_date}),"
        f" FOREIGN KEY ({M_Res_cid}) REFERENCES {M_C_TABLE_NAME}({M_C_id} ) ON DELETE CASCADE ,"
        f" FOREIGN KEY ({M_Res_hid}) REFERENCES {M_A_TABLE_NAME}({M_A_id} ) ON DELETE CASCADE "
        f")",

        f"CREATE TABLE {M_Rev_TABLE_NAME}("
        f" {M_Rev_cid} INTEGER NOT NULL,"
        f" {M_Rev_hid} INTEGER NOT NULL,"
        f" {M_Rev_review_date} DATE NOT NULL,"
        f" {M_Rev_rating} INTEGER NOT NULL,"
        f" {M_Rev_review_text} TEXT NOT NULL,"
        f" PRIMARY KEY({M_Rev_cid}, {M_Rev_hid}),"
        f" FOREIGN KEY ({M_Rev_cid}) REFERENCES {M_C_TABLE_NAME}({M_C_id} ) ON DELETE CASCADE ,"
        f" FOREIGN KEY ({M_Rev_hid}) REFERENCES {M_A_TABLE_NAME}({M_A_id} ) ON DELETE CASCADE "
        f")",

        f"CREATE VIEW {M_RevView_TABLE_NAME} "
        f" AS"
        f" SELECT {M_Rev_TABLE_NAME}.{M_Rev_hid}, {M_OwnedBy_owner_id}, {M_Rev_rating}"
        f" FROM {M_Rev_TABLE_NAME}"
        f" LEFT OUTER JOIN {M_OwnedBy_TABLE_NAME}"
        f" ON {M_Rev_TABLE_NAME}.{M_Rev_hid} = {M_OwnedBy_TABLE_NAME}.{M_OwnedBy_house_id} ",

        f""" CREATE VIEW ViewAptRating AS
        SELECT 
    A.owner_id, 
    A.apartment_id, 
    AVG(COALESCE(R.rating, 0)) AS average_rating 
FROM 
    (SELECT 
        O.owner_id, 
        A.ID AS apartment_id 
    FROM 
        Apartment A 
    LEFT JOIN 
        OwnedBy O ON A.ID = O.apartment_id) AS A 
LEFT JOIN 
    Reviews R ON A.apartment_id = R.apartment_id 
GROUP BY 
    A.owner_id, 
    A.apartment_id;
"""

        f"CREATE VIEW ViewPricePerNight "
        f" AS "
        f" SELECT "
        f" {M_Res_hid},"
        f" AVG( {M_Res_total_price} / (1+{M_Res_end_date}-{M_Res_start_date}) ) as avg_price_per_night"
        f" FROM"
        f" {M_Res_TABLE_NAME} "
        f" GROUP BY {M_Res_hid}",

        f"CREATE VIEW ViewAptValue "
        f" AS"
        f" SELECT "
        f" viewaptrating.apartment_id, average_rating/avg_price_per_night as value"
        f" FROM viewpricepernight LEFT JOIN viewaptrating"
        f" on viewpricepernight.apartment_id=viewaptrating.apartment_id",
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
    conn = None
    for table in ALL_TABLES:
        try:
            conn = Connector.DBConnector()
            query = f"DELETE FROM {table}"
            conn.execute(query)
        except Exception as e:
            print(e)
        finally:
            conn.close()


def drop_tables():
    conn = None
    for table in ALL_TABLES:
        try:
            conn = Connector.DBConnector()
            query = f"DROP TABLE {table} CASCADE"
            conn.execute(query)
        except Exception as e:
            print(e)
        finally:
            conn.close()

    for v in ALL_VIEWS:
        try:
            conn = Connector.DBConnector()
            query = f"DROP VIEW {v}"
            conn.execute(query)
        except Exception as e:
            print(e)
        finally:
            conn.close()
