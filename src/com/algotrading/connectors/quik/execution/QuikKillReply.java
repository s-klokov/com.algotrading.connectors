package com.algotrading.connectors.quik.execution;

import java.util.Objects;

/**
 * Ответ на kill-заявку в терминале QUIK.
 */
public class QuikKillReply {
    /**
     * Момент получения ответа (System.currentTimeMillis()).
     */
    public final long time;
    /**
     * Kill-заявка, для которой пришёл ответ, или {@code null},
     * если пока связь с лимитной заявкой не установлена.
     */
    public final QuikKillOrder quikKillOrder;

    public QuikKillReply(final long time, final QuikKillOrder quikKillOrder) {
        this.time = time;
        this.quikKillOrder = Objects.requireNonNull(quikKillOrder);
    }
}
