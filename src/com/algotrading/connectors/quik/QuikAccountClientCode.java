package com.algotrading.connectors.quik;

import java.util.Objects;

/**
 * Информация об аккаунте и коде клиента.
 */
public class QuikAccountClientCode {

    public final String account;
    public final String clientCode;

    /**
     * Конструктор.
     *
     * @param account аккаунт клиента
     * @param clientCode код клиента
     */
    public QuikAccountClientCode(final String account, final String clientCode) {
        this.account = Objects.requireNonNull(account);
        this.clientCode = (clientCode == null) ? "" : clientCode;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final QuikAccountClientCode that = (QuikAccountClientCode) o;
        return account.equals(that.account) && clientCode.equals(that.clientCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, clientCode);
    }

    @Override
    public String toString() {
        return (clientCode.length() > 0) ? (account + ':' + clientCode) : account;
    }
}
