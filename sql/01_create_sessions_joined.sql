-- 01_create_sessions_joined.sql
-- Purpose: join session, user, flight, and hotel tables into one session-level analytical table
-- Dialect: Spark SQL / Databricks SQL

CREATE OR REPLACE TABLE sessions_joined_sk AS
SELECT
    s.*,

    -- User attributes
    u.birthdate,
    u.gender,
    u.married,
    u.has_children,
    u.home_country,
    u.home_city,
    u.home_airport,
    u.home_airport_lat,
    u.home_airport_lon,
    u.sign_up_date,

    -- Flight attributes
    f.origin_airport,
    f.destination,
    f.destination_airport,
    f.seats,
    f.return_flight_booked,
    f.departure_time,
    f.return_time,
    f.checked_bags,
    f.trip_airline,
    f.destination_airport_lat,
    f.destination_airport_lon,
    f.base_fare_usd,

    -- Hotel attributes
    h.hotel_name,
    h.nights,
    h.rooms,
    h.check_in_time,
    h.check_out_time,
    h.hotel_per_room_usd AS hotel_price_per_room_night_usd

FROM sessions_spark s
LEFT JOIN users_spark u USING (user_id)
LEFT JOIN flights_spark f USING (trip_id)
LEFT JOIN hotels_spark h USING (trip_id);