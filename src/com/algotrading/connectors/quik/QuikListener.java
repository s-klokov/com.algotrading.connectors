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
     *
     * @param exception исключение
     */
    void onExceptionMN(Exception exception);

    /**
     * Вызывается при возникновении ошибки в работе CB-сервера.
     *
     * @param exception исключение
     */
    void onExceptionCB(Exception exception);

    /**
     * @return {@code true}, если бизнес-логика выполняется, иначе {@code false}
     */
    boolean isRunning();

    /**
     * Циклическая реализация бизнес-логики.
     *
     * @param quikConnect   объект для связи с терминалом QUIK
     * @param isInterrupted {@code true}, если стоит флаг прерывания, иначе {@code false}
     */
    void step(QuikConnect quikConnect, boolean isInterrupted);
}
