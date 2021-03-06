package streamer

case class Totals(
    merchandiseCostTotal: Double,
    merchandiseTotal: Double,
    merchandiseTaxTotal: Double,
    shippingTotal: Double,
    shippingTaxTotal: Double,
    discountTotal: Double,
    serviceTotal: Double,
    serviceTaxTotal: Double,
    total: Double
)
case class Dates(
    placedOn: String,
    createdOn: String,
    updatedOn: String
)
case class Attributes(
    webOrderNumber: Option[String],
    webOrderId: Option[Long],
    subsidiary: Option[String],
    channel: Option[String],
    channelId: Option[Long],
    source: Option[String],
    sourceId: Option[Long],
    school: Option[String],
    schoolId: Option[Long]
)
case class Customer(
    email: Option[String],
    id: Long,
    name: String
)

case class Sponsor (
    id: Long,
    name: String
)

case class Billing(
    addressKey: Option[String],
    country: Option[String],
    addr1: Option[String],
    addr2: Option[String],
    city: Option[String],
    state: Option[String],
    zip: Option[String],
    name: Option[String],
    phone: Option[String],
    email: Option[String]
)
case class Line(
    extPrice: Double,
    quantity: Option[Int],
    lob: Option[String],
    cost: Option[Double],
    `type`: String,
    unitPrice: Option[Double],
    shipment: Option[String],
    itemTitle: Option[String],
    itemSku: Option[String],
    itemId: Long,
    isDropship: Boolean,
    isCancelled: Boolean,
    tax: Double,
    shipping: Double,
    discount: Double,
    lineId: Option[String],
    uniqueKey: Option[String],
    packageId: Option[Long]
)
case class Shipments(
    addressKey: Option[String],
    lines: List[Line],
    addr1: Option[String],
    addr2: Option[String],
    city: Option[String],
    state: Option[String],
    zip: Option[String],
    name: Option[String],
    phone: Option[String],
    email: Option[String],
    `type`: Option[String],
    programId: Option[Int],
    programName: Option[String],
    sponsorId: Option[Int],
    sponsorName: Option[String]
)
case class NetsuiteOrder(
    id: Long,
    orderNumber: String,
    totals: Totals,
    dates: Dates,
    attributes: Attributes,
    customer: Customer,
    billing: Billing,
    fees: List[Line],
    shipments: List[Shipments],
    sponsor_1: Option[Long],
    sponsor_2: Option[Long],
    sponsor_3: Option[Long],
    sponsor_4: Option[Long],
    sponsor_5: Option[Long],
    sponsorname_1: Option[String],
    sponsorname_2: Option[String],
    sponsorname_3: Option[String],
    sponsorname_4: Option[String],
    sponsorname_5: Option[String],    
    `type`: String,
    status: String,
)