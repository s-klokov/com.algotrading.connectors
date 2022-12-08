package com.algotrading.connectors.quik;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.simpleutils.Deduplicator.STRING_DEDUPLICATOR;
import static com.simpleutils.json.JSONConfig.getStringNonNull;

/**
 * Информация о клиенте в терминале QUIK:
 * идентификатор клиента и информация о счетах для торговли различных классов инструментов.
 */
public class QuikClient {
    /**
     * Идентификатор клиента.
     */
    public final String id;
    /**
     * Соответствие: код класса -> используемые для торговли аккаунт и код клиента.
     */
    private final Map<String, QuikAccountClientCode> map;

    /**
     * Конструктор.
     *
     * @param id    идентификатор клиента
     * @param array объект, содержащий информацию об идентификаторе клиента
     *              и его аккаунтах и кодах для разных классов инструментов
     */
    public QuikClient(final String id, final JSONArray array) {
        this.id = Objects.requireNonNull(id);
        map = new HashMap<>();
        for (final Object o : array) {
            final JSONObject json = (JSONObject) o;
            final QuikAccountClientCode quikAccountClientCode = new QuikAccountClientCode(
                    getStringNonNull(json, "account"),
                    getStringNonNull(json, "clientCode")
            );
            final String[] classCodes = getStringNonNull(json, "classCodes").split(",");
            for (final String classCode : classCodes) {
                if (!classCode.isBlank()) {
                    map.put(STRING_DEDUPLICATOR.deduplicate(classCode), quikAccountClientCode);
                }
            }
        }
    }

    /**
     * @param classCode код класса
     * @return аккаунт и код клиента для этого класса
     */
    public QuikAccountClientCode getQuikAccountClientCode(final String classCode) {
        return map.get(classCode);
    }
}
