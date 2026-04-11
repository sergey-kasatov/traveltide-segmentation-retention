-- 03_create_user_agg_features.sql
-- Purpose: aggregate cleaned session-level data into user-level behavioral features
-- Dialect: Spark SQL / Databricks SQL

CREATE OR REPLACE TABLE workspace.default.user_agg_features AS

WITH max_date AS (
    SELECT MAX(session_start) AS ref_date
    FROM workspace.default.sessions_filtered_cleaned
),

session_features AS (
    SELECT
        s.*,

        -- Distance from home airport to destination airport (km)
        CASE
            WHEN s.flight_booked = TRUE
                 AND s.cancellation = FALSE
                 AND s.home_airport_lat IS NOT NULL
                 AND s.home_airport_lon IS NOT NULL
                 AND s.destination_airport_lat IS NOT NULL
                 AND s.destination_airport_lon IS NOT NULL
            THEN
                2 * 6371 * ASIN(
                    SQRT(
                        POWER(SIN(RADIANS(s.destination_airport_lat - s.home_airport_lat) / 2), 2) +
                        COS(RADIANS(s.home_airport_lat)) *
                        COS(RADIANS(s.destination_airport_lat)) *
                        POWER(SIN(RADIANS(s.destination_airport_lon - s.home_airport_lon) / 2), 2)
                    )
                )
            ELSE NULL
        END AS trip_distance_km,

        -- Trip duration in days
        CASE
            WHEN s.departure_time IS NOT NULL
                 AND s.return_time IS NOT NULL
                 AND s.return_time > s.departure_time
            THEN (unix_timestamp(s.return_time) - unix_timestamp(s.departure_time)) / 86400.0
            ELSE NULL
        END AS trip_duration_days,

        -- Total discount amount per session
        COALESCE(s.flight_discount_amount, 0) + COALESCE(s.hotel_discount_amount, 0) AS total_discount_amount

    FROM workspace.default.sessions_filtered_cleaned s
),

user_agg_features AS (
    SELECT
        sf.user_id,

        -- Activity / engagement
        COUNT(*) AS total_sessions,
        SUM(sf.page_clicks) AS total_page_clicks,
        ROUND(AVG(sf.page_clicks), 2) AS avg_page_clicks_per_session,

        SUM(CASE WHEN sf.trip_id IS NOT NULL THEN 1 ELSE 0 END) AS trip_session_count,
        ROUND(
            SUM(CASE WHEN sf.trip_id IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            3
        ) AS trip_sessions_share,

        ROUND(
            AVG(CASE WHEN sf.trip_id IS NOT NULL THEN sf.page_clicks END),
            2
        ) AS avg_clicks_trip_sessions,

        -- Booking / conversion counts
        SUM(CASE WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END) AS completed_flight_bookings,
        SUM(CASE WHEN sf.hotel_booked = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END) AS completed_hotel_bookings,
        SUM(CASE WHEN sf.any_booking = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END) AS completed_any_bookings,
        SUM(CASE WHEN sf.cancellation = TRUE THEN 1 ELSE 0 END) AS cancellation_count,

        -- Conversion rates
        ROUND(
            SUM(CASE WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            3
        ) AS flight_booking_rate,

        ROUND(
            SUM(CASE WHEN sf.hotel_booked = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            3
        ) AS hotel_booking_rate,

        ROUND(
            SUM(CASE WHEN sf.any_booking = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            3
        ) AS overall_booking_rate,

        ROUND(
            SUM(CASE WHEN sf.cancellation = TRUE THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            3
        ) AS cancellation_rate,

        ROUND(
            SUM(
                CASE
                    WHEN sf.flight_booked = TRUE
                         AND sf.hotel_booked = TRUE
                         AND sf.cancellation = FALSE
                    THEN 1 ELSE 0
                END
            ) / NULLIF(
                SUM(CASE WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END),
                0
            ),
            3
        ) AS hotel_attach_rate,

        ROUND(
            SUM(
                CASE
                    WHEN sf.return_flight_booked = TRUE
                         AND sf.flight_booked = TRUE
                         AND sf.cancellation = FALSE
                    THEN 1 ELSE 0
                END
            ) / NULLIF(
                SUM(CASE WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END),
                0
            ),
            3
        ) AS return_flight_booking_rate,

        -- Recency / activity span
        DATEDIFF(md.ref_date, MAX(sf.session_start)) AS days_since_last_session,

        DATEDIFF(
            md.ref_date,
            MAX(
                CASE
                    WHEN sf.any_booking = TRUE
                         AND sf.cancellation = FALSE
                    THEN sf.session_start
                END
            )
        ) AS days_since_last_booking,

        DATEDIFF(MAX(sf.session_start), MIN(sf.session_start)) AS active_span_days,

        -- Frequency normalized by tenure
        FLOOR(months_between(md.ref_date, MIN(sf.sign_up_date))) AS customer_age_months,

        ROUND(
            COUNT(*) / NULLIF(months_between(md.ref_date, MIN(sf.sign_up_date)), 0),
            2
        ) AS sessions_per_month,

        ROUND(
            SUM(CASE WHEN sf.any_booking = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END)
            / NULLIF(months_between(md.ref_date, MIN(sf.sign_up_date)), 0),
            2
        ) AS bookings_per_month,

        -- Spend / value
        ROUND(
            AVG(
                CASE
                    WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.base_fare_usd
                END
            ),
            2
        ) AS avg_base_fare_usd,

        ROUND(
            AVG(
                CASE
                    WHEN sf.hotel_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.hotel_price_per_room_night_usd
                END
            ),
            2
        ) AS avg_hotel_price_per_room_night_usd,

        ROUND(
            AVG(
                CASE
                    WHEN sf.hotel_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.nights
                END
            ),
            2
        ) AS avg_nights,

        ROUND(
            AVG(
                CASE
                    WHEN sf.hotel_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.nights_fixed
                END
            ),
            2
        ) AS avg_nights_fixed,

        COUNT(DISTINCT
            CASE
                WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE
                THEN sf.trip_airline
            END
        ) AS unique_airlines_booked,

        -- Distance / duration / travel behavior
        ROUND(AVG(sf.trip_distance_km), 2) AS avg_trip_distance_km,
        ROUND(MAX(sf.trip_distance_km), 2) AS max_trip_distance_km,
        ROUND(SUM(sf.trip_distance_km), 2) AS total_trip_distance_km,

        ROUND(
            AVG(
                CASE
                    WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.trip_duration_days
                END
            ),
            2
        ) AS avg_trip_duration_days,

        ROUND(
            MAX(
                CASE
                    WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.trip_duration_days
                END
            ),
            2
        ) AS max_trip_duration_days,

        COUNT(DISTINCT
            CASE
                WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE
                THEN sf.destination
            END
        ) AS unique_destinations_count,

        ROUND(
            AVG(
                CASE
                    WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE
                    THEN HOUR(sf.departure_time)
                END
            ),
            2
        ) AS avg_departure_hour,

        ROUND(
            SUM(
                CASE
                    WHEN sf.flight_booked = TRUE
                         AND sf.cancellation = FALSE
                         AND DAYOFWEEK(sf.departure_time) IN (1, 7)
                    THEN 1 ELSE 0
                END
            ) / NULLIF(
                SUM(CASE WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE THEN 1 ELSE 0 END),
                0
            ),
            3
        ) AS weekend_trip_share,

        -- Optional travel behavior
        ROUND(
            AVG(
                CASE
                    WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.checked_bags
                END
            ),
            2
        ) AS avg_checked_bags,

        ROUND(
            AVG(
                CASE
                    WHEN sf.flight_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.seats
                END
            ),
            2
        ) AS avg_seats_booked,

        ROUND(
            AVG(
                CASE
                    WHEN sf.hotel_booked = TRUE AND sf.cancellation = FALSE
                    THEN sf.rooms
                END
            ),
            2
        ) AS avg_rooms_booked,

        -- Discount behavior
        ROUND(
            SUM(CASE WHEN sf.flight_discount = TRUE THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            3
        ) AS flight_discount_seen_rate,

        ROUND(
            SUM(CASE WHEN sf.hotel_discount = TRUE THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            3
        ) AS hotel_discount_seen_rate,

        ROUND(
            SUM(
                CASE
                    WHEN sf.flight_discount = TRUE
                         AND sf.flight_booked = TRUE
                         AND sf.cancellation = FALSE
                    THEN 1 ELSE 0
                END
            ) / NULLIF(
                SUM(CASE WHEN sf.flight_discount = TRUE THEN 1 ELSE 0 END),
                0
            ),
            3
        ) AS flight_discount_booking_rate,

        ROUND(
            SUM(
                CASE
                    WHEN sf.hotel_discount = TRUE
                         AND sf.hotel_booked = TRUE
                         AND sf.cancellation = FALSE
                    THEN 1 ELSE 0
                END
            ) / NULLIF(
                SUM(CASE WHEN sf.hotel_discount = TRUE THEN 1 ELSE 0 END),
                0
            ),
            3
        ) AS hotel_discount_booking_rate,

        ROUND(
            AVG(
                CASE
                    WHEN sf.flight_discount = TRUE
                    THEN sf.flight_discount_amount
                END
            ),
            2
        ) AS avg_flight_discount_amount,

        ROUND(
            AVG(
                CASE
                    WHEN sf.hotel_discount = TRUE
                    THEN sf.hotel_discount_amount
                END
            ),
            2
        ) AS avg_hotel_discount_amount,

        ROUND(AVG(sf.total_discount_amount), 2) AS avg_total_discount_amount,

        -- Demographics
        FLOOR(months_between(md.ref_date, MAX(sf.birthdate)) / 12) AS age_at_dataset_end,
        MAX(sf.gender) AS gender,
        MAX(CASE WHEN sf.married = TRUE THEN TRUE ELSE FALSE END) AS married,
        MAX(CASE WHEN sf.has_children = TRUE THEN TRUE ELSE FALSE END) AS has_children,

        -- Geography
        MAX(sf.home_country) AS home_country,
        MAX(sf.home_city) AS home_city,
        MAX(sf.home_airport) AS home_airport

    FROM session_features sf
    CROSS JOIN max_date md
    GROUP BY sf.user_id, md.ref_date
)

SELECT *
FROM user_agg_features;