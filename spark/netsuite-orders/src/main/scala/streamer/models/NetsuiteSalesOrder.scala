package streamer.models

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
  webOrderId: Option[Double],
  subsidiary: Option[String],
  channel: Option[String],
  source: Option[String],
  school: Option[String]
)
case class Customer(
  email: Option[String],
  id: String,
  name: String
)
case class Billing(
  country: Option[String],
  addr1: Option[String],
  addr2: Option[String],
  city: Option[String],
  state: Option[String],
  zip: Option[String],
  name: Option[String],
  phone: Option[String]
)
case class Line(
  extPrice: Double,
  quantity: String,
  lob: String,
  cost: String,
  `type`: String,
  unitPrice: Double,
  shipment: String,
  itemTitle: String,
  itemSku: String,
  itemId: Double,
  isDropship: Boolean,
  isCancelled: Boolean,
  tax: Double
)
case class Shipments(
  lines: List[Line],
  addr1: Option[String],
  addr2: Option[String],
  city: Option[String],
  state: Option[String],
  zip: Option[String],
  name: Option[String],
  phone: Option[String],
  email: Option[String],
  `type`: Option[String]
)
case class NetsuiteOrder(
  id: Double,
  orderNumber: String,
  totals: Totals,
  dates: Dates,
  attributes: Attributes,
  customer: Customer,
  billing: Billing,
  fees: List[Line],
  shipments: List[Shipments]
)


