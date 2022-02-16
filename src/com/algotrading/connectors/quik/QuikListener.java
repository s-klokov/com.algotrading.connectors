package com.algotrading.connectors.quik;

import org.json.simple.JSONObject;

/**
 * Слушатель событий от терминала QUIK.
 */
public interface QuikListener {

    /**
     * Задать используемое подключение к терминалу QUIK.
     * <p>
     * Этот метод нужно вызвать перед методом {@link QuikConnect#start()}.
     *
     * @param quikConnect подключение к терминалу QUIK
     */
    void setQuikConnect(final QuikConnect quikConnect);

    /**
     * @return {@code true}, если бизнес-логика выполняется, иначе {@code false}
     */
    boolean isRunning();

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
     * Циклическая реализация бизнес-логики.
     *
     * @param quikConnect   объект для связи с терминалом QUIK
     * @param isInterrupted {@code true}, если стоит флаг прерывания, иначе {@code false}
     */
    void step(boolean isInterrupted);
}
