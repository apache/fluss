/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.source.testutils;

import java.util.Objects;

/**
 * Represents an order used for testing.
 *
 * <p>This class contains information about an order including its ID, the item being ordered, the
 * quantity ordered, and the shipping address.
 */
public class Order {
    private long orderId;
    private long itemId;
    private int amount;
    private String address;

    /** Default constructor for creating an empty order. */
    public Order() {}

    /**
     * Constructs an order with the specified details.
     *
     * @param orderId the unique identifier for this order
     * @param itemId the identifier of the item being ordered
     * @param amount the quantity of the item being ordered
     * @param address the shipping address for this order
     */
    public Order(long orderId, long itemId, int amount, String address) {
        this.orderId = orderId;
        this.itemId = itemId;
        this.amount = amount;
        this.address = address;
    }

    /**
     * Gets the order identifier.
     *
     * @return the order's unique identifier
     */
    public long getOrderId() {
        return orderId;
    }

    /**
     * Sets the order identifier.
     *
     * @param orderId the order's unique identifier
     */
    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    /**
     * Gets the item identifier.
     *
     * @return the identifier of the ordered item
     */
    public long getItemId() {
        return itemId;
    }

    /**
     * Sets the item identifier.
     *
     * @param itemId the identifier of the ordered item
     */
    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    /**
     * Gets the ordered amount/quantity.
     *
     * @return the quantity of the item ordered
     */
    public int getAmount() {
        return amount;
    }

    /**
     * Sets the ordered amount/quantity.
     *
     * @param amount the quantity of the item ordered
     */
    public void setAmount(int amount) {
        this.amount = amount;
    }

    /**
     * Gets the shipping address.
     *
     * @return the shipping address for this order
     */
    public String getAddress() {
        return address;
    }

    /**
     * Sets the shipping address.
     *
     * @param address the shipping address for this order
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /**
     * Compares this order with the specified object for equality.
     *
     * <p>Returns true if the given object is also an Order and the two Orders have the same
     * orderId, itemId, amount, and address.
     *
     * @param o the object to compare this order with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Order order = (Order) o;
        return orderId == order.orderId
                && itemId == order.itemId
                && amount == order.amount
                && Objects.equals(address, order.address);
    }

    /**
     * Returns a hash code value for this order.
     *
     * <p>The hash code is based on the orderId, itemId, amount, and address fields.
     *
     * @return a hash code value for this order
     */
    @Override
    public int hashCode() {
        return Objects.hash(orderId, itemId, amount, address);
    }

    /**
     * Returns a string representation of this order.
     *
     * <p>The returned string contains all fields of the order in a human-readable format.
     *
     * @return a string representation of this order
     */
    @Override
    public String toString() {
        return "Order{"
                + "order_id="
                + orderId
                + ", item_id="
                + itemId
                + ", amount="
                + amount
                + ", address='"
                + address
                + '\''
                + '}';
    }
}
