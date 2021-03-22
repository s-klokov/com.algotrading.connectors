package com.algotrading.connectors.quik;

import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Интерфейс для взаимодействия с терминалом QUIK.
 */
public interface QuikInterface {

    /**
     * Выполнить настройку объекта на основании JSON-конфигурации (JSON-объект или JSONArray).
     *
     * @param config конфигурация
     */
    void configurate(JSONAware config);

    /**
     * Запуск.
     */
    void init();

    /**
     * Остановка.
     */
    void shutdown();

    /**
     * @return {@code true}, если бизнес-логика выполняется, иначе {@code false}
     */
    boolean isRunning();

    /**
     * @return {@code true}, если была ошибка взаимодействия с MN-сервером, иначе {@code false}
     */
    boolean hasErrorMN();

    /**
     * @return {@code true}, если была ошибка взаимодействия с CB-сервером, иначе {@code false}
     */
    boolean hasErrorCB();

    /**
     * Реализация бизнес-логики.
     *
     * @param isInterrupted {@code true}, если стоит флаг прерывания, иначе {@code false}
     * @param hasErrorMN    {@code true}, если была ошибка взаимодействия с MN-сервером, иначе {@code false}
     * @param hasErrorCB    {@code true}, если была ошибка взаимодействия с CB-сервером, иначе {@code false}
     */
    void step(boolean isInterrupted, boolean hasErrorMN, boolean hasErrorCB);

    /**
     * Вызывается после установления соединения с терминалом.
     */
    void onOpen();

    /**
     * Вызывается после закрытия соединения с терминалом.
     */
    void onClose();

    /**
     * Вызывается при получении ответа от MN-сервера.
     *
     * @param jsonObject json-объект
     */
    void onReceiveMN(JSONObject jsonObject);

    /**
     * Вызывается при получении ответа от CB-сервера.
     *
     * @param jsonObject json-объект
     */
    void onReceiveCB(JSONObject jsonObject);

    /**
     * Вызывается при возникновении ошибки ввода-вывода при работе с MN-сервером.
     *
     * @param e исключение
     */
    void onIOExceptionMN(IOException e);

    /**
     * Вызывается при возникновении ошибки ввода-вывода при работе с CB-сервером.
     *
     * @param e исключение
     */
    void onIOExceptionCB(IOException e);

    /**
     * Вызывается при возникновении исключений, отличных от ошибок ввода-вывода.
     *
     * @param e исключение
     */
    void onException(Exception e);

    /**
     * Отправить chunk-запрос MN-серверу.
     *
     * @param chunk код запроса на языке QLua
     * @throws IOException если произошла ошибка ввода-вывода
     */
    void sendMN(String chunk) throws IOException;

    /**
     * Отправить function-запрос MN-серверу.
     *
     * @param fname имя QLua-функции
     * @param args  список аргументов функции
     * @throws IOException если произошла ошибка ввода-вывода
     */
    void sendMN(String fname, List<?> args) throws IOException;

    /**
     * Отправить запрос CB-серверу.
     *
     * @param callback имя коллбэка
     * @param filter   код функции фильтрации на языке QLua
     * @throws IOException если произошла ошибка ввода-вывода
     */
    void sendCB(String callback, String filter) throws IOException;

    /**
     * Отправить chunk-запрос MN-серверу и получить будущий ответ.
     *
     * @param chunk   код запроса на языке QLua
     * @param timeout таймаут ожидания
     * @param unit    единица измерения времени
     * @return будущий ответ MN-сервера
     */
    CompletableFuture<JSONObject> getResponseMN(String chunk,
                                                long timeout, TimeUnit unit);

    /**
     * Отправить function-запрос MN-серверу и получить будущий ответ.
     *
     * @param fname   имя QLua-функции
     * @param args    список аргументов функции
     * @param timeout таймаут ожидания
     * @param unit    единица измерения времени
     * @return будущий ответ MN-сервера
     */
    CompletableFuture<JSONObject> getResponseMN(String fname, List<?> args,
                                                long timeout, TimeUnit unit);

    /**
     * Отправить запрос CB-серверу и получить будущий ответ.
     *
     * @param callback имя коллбэка
     * @param filter   код функции фильтрации на языке QLua
     * @param timeout  таймаут ожидания
     * @param unit     единица измерения времени
     * @return будущий ответ CB-сервера
     */
    CompletableFuture<JSONObject> getResponseCB(String callback, String filter,
                                                long timeout, TimeUnit unit);

}
