package com.algotrading.connectors.quik;

import org.json.simple.JSONObject;

/**
 * Слушатель событий от терминала QUIK.
 */
public interface QuikListener {

    /**
     * Задать используемое подключение к терминалу QUIK.
     * <p>
     * Порядок вызова:<br>
     * 1) этот метод;<br>
     * 2) метод {@link QuikConnect#start()};<br>
     * 3) поток слушателя: {@code listener.getExecutionThread().start()}.
     *
     * @param quikConnect подключение к терминалу QUIK
     */
    void setQuikConnect(final QuikConnect quikConnect);

    /**
     * @return поток, в котором выполняется бизнес-логика слушателя
     */
    Thread getExecutionThread();

    /**
     * Поставить код в очередь на исполнение в потоке бизнес-логики слушателя.
     *
     * @param runnable исполняемый код
     */
    void submit(Runnable runnable);

    /**
     * Вызывается после установления соединения с терминалом в потоке, где работает {@link QuikConnect}.
     */
    void onOpen();

    /**
     * Вызывается после закрытия соединения с терминалом в потоке, где работает {@link QuikConnect}.
     */
    void onClose();

    /**
     * Вызывается при получении коллбэка от терминала QUIK в потоке, где работает {@link QuikConnect}.
     *
     * @param jsonObject объект, описывающий коллбэк
     */
    void onCallback(JSONObject jsonObject);

    /**
     * Вызывается при возникновении ошибки в работе MN-сервера в потоке, где работает {@link QuikConnect}.
     *
     * @param exception исключение
     */
    void onExceptionMN(Exception exception);

    /**
     * Вызывается при возникновении ошибки в работе CB-сервера в потоке, где работает {@link QuikConnect}.
     *
     * @param exception исключение
     */
    void onExceptionCB(Exception exception);
}
