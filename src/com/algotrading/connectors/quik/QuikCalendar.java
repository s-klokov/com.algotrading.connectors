package com.algotrading.connectors.quik;

import com.simpleutils.json.JSONConfig;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.time.DayOfWeek;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Календарь, задающий даты и время работы терминала QUIK.
 */
public class QuikCalendar {

    private final ZoneId zoneId;
    private final long[] holidays;
    private final long[] workdays;
    private final long hhmmssFrom;
    private final long hhmmssTill;
    private ZonedDateTime dt = ZonedDateTime.now();

    /**
     * Конструктор.
     *
     * @param config json-объект с настройками
     */
    public QuikCalendar(final JSONObject config) {
        zoneId = ZoneId.of(JSONConfig.getOrDefault(config, "zoneId", "Europe/Moscow"));
        JSONArray array = (JSONArray) config.get("holidays");
        holidays = new long[array.size()];
        for (int i = 0; i < holidays.length; i++) {
            holidays[i] = (long) array.get(i);
        }
        array = (JSONArray) config.get("workdays");
        workdays = new long[array.size()];
        for (int i = 0; i < workdays.length; i++) {
            workdays[i] = (long) array.get(i);
        }
        hhmmssFrom = JSONConfig.getOrDefault(config, "hhmmssFrom", 9_40_00);
        hhmmssTill = JSONConfig.getOrDefault(config, "hhmmssTill", 23_55_00);
        updateDateTime();
    }

    /**
     * Узнать, является ли текущий момент времени рабочим.
     *
     * @return {@code true/false}
     */
    public boolean isWorking() {
        updateDateTime();
        final int hhmmss = dt.getHour() * 10000 + dt.getMinute() * 100 + dt.getSecond();
        if (hhmmssFrom <= hhmmssTill) {
            // Происходит внутри одних суток
            return hhmmssFrom <= hhmmss && hhmmss < hhmmssTill && isWorkingDate(dt);
        } else {
            // Начинается в одни сутки и заканчивается в следующие сутки
            return hhmmss >= hhmmssFrom && isWorkingDate(dt)
                   || hhmmss < hhmmssTill && isWorkingDate(dt.minusDays(1));
        }
    }

    private void updateDateTime() {
        final ZonedDateTime now = ZonedDateTime.now(zoneId);
        if (now.isAfter(dt)) {
            dt = now;
        }
    }

    private static boolean contains(final long[] array, final long value) {
        for (final long v : array) {
            if (v == value) {
                return true;
            }
        }
        return false;
    }

    private boolean isWorkingDate(final ZonedDateTime dt) {
        final int yyyymmdd = dt.getYear() * 10000 + dt.getMonthValue() * 100 + dt.getDayOfMonth();
        final DayOfWeek dayOfWeek = dt.getDayOfWeek();
        if (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY) {
            return contains(workdays, yyyymmdd);
        } else {
            return !contains(holidays, yyyymmdd);
        }
    }
}
