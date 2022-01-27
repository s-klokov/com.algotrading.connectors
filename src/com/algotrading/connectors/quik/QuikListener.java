package com.algotrading.connectors.quik;

import org.json.simple.JSONObject;

/**
 * Слушатель событий от терминала QUIK.
 */
public interface QuikListener {

    /**
     * Вызывается после установления соединения с терминалом.
     */
    void onOpen();

    /**
     * Вызывается после закрытия соединения с терминалом.
     */
    void onClose();

    /**
     * Вызывается при получении коллбэка от терминала QUIK.
     *
     * @param jsonObject объект, описывающий коллбэк
     */
    void onCallback(JSONObject jsonObject);

    /**
     * Вызывается при возникновении ошибки в работе MN-сервера.
     * @param exception исключение
     */
    void onExceptionMN(Exception exception);

    /**
     * Вызывается при возникновении ошибки в работе CB-сервера.
     * @param exception исключение
     */
    void onExceptionCB(Exception exception);
}
