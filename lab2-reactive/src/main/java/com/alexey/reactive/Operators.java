package com.alexey.reactive;

final class Operators {

    private Operators() {
    }

    static void link(Disposable registry, Disposable child) {
        if (registry instanceof SubscriptionRegistry bag) {
            bag.track(child);
        }
    }
}
