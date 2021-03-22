package com.algotrading.connectors.quik;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.algotrading.base.util.Deduplicator.STRING_DEDUPLICATOR;
import static com.algotrading.base.util.JSONConfig.getStringNonNull;

/**
 * Информация о клиенте в терминале QUIK.
 * <p>
 * Пример задания информации о клиенте в json-формате, где для каждого
 * идентификатора клиента указываются его счёта и аккаунты для всех торгуемых
 * кодов классов.
 * <pre>
 * {
 *   "210": {
 *     "L01+00000F00:210": "TQBR,TQOB,TQTF",
 *     "SPBFUT00210": "SPBFUT,SPBOPT,OPTW",
 *     "L75+00000F00:K0005_210": "SPBXM",
 *   },
 *   "D25": {
 *     "SPBFUT00D25": "SPBFUT,SPBOPT,OPTW",
 *   },
 * }
 * </pre>
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
