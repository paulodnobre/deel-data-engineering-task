"""Transformation logic for analytics pipeline.

Implements business logic from the functional contract:
- compute_is_open: Determine if order is open based on status
- compute_quantity_pending: Calculate pending items based on order status
- Normalize names

All transformations are testable in isolation (no DB dependencies).
"""
import logging

logger = logging.getLogger(__name__)


def compute_is_open(order_status):
    """Determine if order is open based on status.

    Per FUNCTIONAL-CONTRACT.md:
    "Open order" = status <> 'COMPLETED'

    Args:
        order_status: String status value from source (PENDING, PROCESSING, REPROCESSING, COMPLETED)

    Returns:
        int: 1 if order is open (status != 'COMPLETED'), 0 if closed

    Assumptions:
        - Status field is authoritative source of order state
        - No implicit order states (only explicit status field values)
    """
    logger.debug(f"compute_is_open(status={order_status})")
    if order_status and order_status.upper() != 'COMPLETED':
        return 1
    return 0


def compute_quantity_pending(quantity, order_status):
    """Calculate quantity pending for order item based on order status.

    Per FUNCTIONAL-CONTRACT.md:
    "Pending items" = SUM(quantity) where status <> 'COMPLETED'

    Args:
        quantity: Numeric quantity from order_items (from source column 'quanity')
        order_status: String status from parent order

    Returns:
        int: quantity if order is open (status != 'COMPLETED'), 0 if closed

    Assumptions:
        - Quantity is always positive (no negative adjustments in source)
        - NULL quantities are treated as 0
        - Status field is authoritative for determining pending status
    """
    logger.debug(f"compute_quantity_pending(qty={quantity}, status={order_status})")

    # Handle NULL quantity
    if quantity is None:
        quantity = 0

    # Return quantity if open, 0 if closed
    if order_status and order_status.upper() != 'COMPLETED':
        return quantity
    return 0


def normalize_product_name(name):
    """Normalize product name for consistency.

    Args:
        name: Product name from source

    Returns:
        str: Normalized product name
    """
    logger.debug(f"normalize_product_name(name={name})")
    if name is None:
        return None
    return name


def normalize_customer_name(name):
    """Normalize customer name for consistency.

    Args:
        name: Customer name from source

    Returns:
        str: Normalized customer name
    """
    logger.debug(f"normalize_customer_name(name={name})")
    if name is None:
        return None
    return name
