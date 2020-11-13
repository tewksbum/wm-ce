package streamer

case class OrdersFact(
    date_key: Long,
    orderstatus_key: Long,
    ordertype_key: Long,
    channel_key: Long,
    source_key: Long,
    school_key: Long,
    sponsor_key_1: Option[Long],
    sponsor_key_2: Option[Long],
    sponsor_key_3: Option[Long],
    sponsor_key_4: Option[Long],
    sponsor_key_5: Option[Long],
    customer_key: String,
    billto_key: String,
    netsuite_id: Long,
    netsuite_number: String,
    ocm_id: Long,
    ocm_number: String,
    merchandise_cost: Double,
    merchandise_total: Double,
    merchandise_tax: Double,
    shipping: Double,
    shipping_tax: Double,
    discount: Double,
    service: Double,
    service_tax: Double,
    total: Double
)

case class OrderLineFact(
    date_key: Long,
    orderstatus_key: Long,
    ordertype_key: Long,    
    channel_key: Long,
    source_key: Long,
    school_key: Long,
    sponsor_key: Long,    
    program_key: Long,
    customer_key: String,
    product_key: Long,
    billto_key: String,
    shipto_key: String,
    netsuite_order_id: Long,
    netsuite_order_number: String,
    ocm_order_id: Long,
    ocm_order_number: String,
    shipment_number: String,
    netsuite_line_id: String,
    netsuite_line_key: String,
    lob_key: Long,
    desttype_key: Long,
    is_dropship: Int,
    total_price: Double,
    total_tax: Double,
    total_cost: Double,
    total_discount: Double,
    total_shipping: Double,
    quantity: Int,
    is_discount: Int,
    is_shipping: Int,
    is_service: Int,
    is_cancelled: Int
)

case class DailySalesFact(
    schedule_key: Long,
    date_key: Long,
    channel_key: Long,
    lob_key: Long,
    is_dropship: Int,
    price: Double,
    cost: Double,
    tax: Double,
    discount: Double,
    shipping: Double
)

case class DateDim( // no need to update, read only, also we dont need all the columns
    date_key: Long,
    date_string: String
)

case class ProductDim( // this dim probably get updated by some type of feed
    product_key: Long,
    sku: String,
    lob_key: Long,
    netsuite_id: Long
)

case class LOBDim( // no need to update, read only
    lob_key: Long,
    lob_name: String
)

case class SchoolDim( // this dim probably get updated by some type of feed
    school_key: Long,
    school_code: String,
    school_name: String,
    netsuite_id: Long
)

case class SourceDim( // no need to update, read only
    source_key: Long,
    source_name: String,
    netsuite_id: Long
)

case class ChannelDim( // no need to update, read only
    channel_key: Long,
    channel_name: String,
    netsuite_id: Long
)

case class BillToDim(
    billto_key: String,
    netsuite_key: String,
    name: String,
    addr1: String,
    addr2: String,
    city: String,
    state: String,
    zip: String,
    country: String,
    phone: String,
    email: String,
    owner: Option[String],
    source: Option[String],
    event_id: Option[String],
    event_type: Option[String],
    address_type: Option[String]
)

case class Pipeline (
    owner: String,
    source: String,
    eventType: String,
    eventId: String,
    recordId: String,
    name: String,
    addr1: String,
    addr2: String,
    city: String,
    state: String,
    zip: String,
    country: String,
    phone: String,
    email: String  
)

case class ShipToDim(
    shipto_key: String,
    netsuite_key: String,
    name: String,
    addr1: String,
    addr2: String,
    city: String,
    state: String,
    zip: String,
    country: String,
    phone: String,
    desttype_key: Long,
    email: String,
    owner: Option[String],
    source: Option[String],
    event_id: Option[String],
    event_type: Option[String],
    address_type: Option[String]
)

case class CustomerDim(
    customer_key: String,
    customer_name: String,
    customer_email: String,
    netsuite_id: Long
)

case class DestTypeDim(
    desttype_key: Long,
    desttype_name: String
)

case class LineUpsertResult (
    line_id: String,
    existing: Boolean,
    cancelled: Boolean,
    price: Double,
    tax: Double,
    cost: Double,
    discount: Double,
    shipping: Double
)

case class PeopleUpsertResult (
    old_key: String,
    new_key: String
)

case class SponsorDim (
    sponsor_key: Long,
    sponsor_code: String,
    sponsor_name: String,
    netsuite_id: Long,
    school_key: Option[Long]
)

case class OrderTypeDim (
    ordertype_key: Long,
    ordertype_name: String,
    include_in_dsr: Boolean
)

case class OrderStatusDim (
    orderstatus_key: Long,
    orderstatus_name: String,
    include_in_dsr: Boolean
)