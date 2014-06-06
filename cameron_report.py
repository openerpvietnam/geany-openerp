from report import report_sxw
from report.report_sxw import rml_parse
from osv import fields, osv
import pooler


class Parser(report_sxw.rml_parse):
    def __init__(self, cr, uid, name, context):
        super(Parser, self).__init__(cr, uid, name, context)

        self.localcontext.update({
            'get_detail': self._get_detail,
            'get_total': self._get_total
        })

    def _get_detail(self, obj):
        orderid = obj.id

        sql = """
         WITH get_qty_untax as(
         SELECT pp.default_code as part_no , sl.name as description, sl.product_uom_qty as quantity, pt.weight_net,sl.product_uom_qty as product_uom_qty, sl.price_unit as price_unit, sl.id as id
          
         FROM sale_order so, sale_order_line sl,product_product pp , product_template pt
         WHERE
         sl.order_id = so.id and sl.product_id = pp.id  and pp.product_tmpl_id = pt.id and sl.order_id = %s
         ),
         get_qty as
         (
         select part_no, gqu.description , gqu.quantity , gqu.weight_net, gqu.price_unit, sm.sale_line_id as sale_order_line, sot.order_line_id as order_line_id, at.id as atid,
         (CASE 
         WHEN price_include = 't' THEN gqu.price_unit
         WHEN price_include = 'f' THEN gqu.price_unit*(1+at.amount)
         ELSE gqu.price_unit
         END) as retail_price,
         (CASE
         WHEN price_include = 'f' THEN gqu.price_unit
         WHEN price_include = 't' THEN gqu.price_unit/(1 + at.amount)
         ELSE  gqu.price_unit
         END) as cost_price
         from get_qty_untax gqu left join sale_order_tax sot on gqu.id = sot.order_line_id
         left join account_tax at on sot.tax_id = at.id
         left join stock_move sm on gqu.id = sm.sale_line_id
         ),
         
         qty_on as(
         SELECT sum(product_qty) as qty_on, sale_line_id,product_id
         FROM stock_move
         WHERE state = 'done'
         GROUP BY sale_line_id,product_id 
         ),
         
         tbltam as(
         SELECT part_no,description, quantity, weight_net, price_unit, sale_order_line, order_line_id, atid, retail_price, cost_price,
         (case
         when qty_on is null then 0
         else qty_on
         end) as qty_on
       
         FROM get_qty gq left join qty_on qo  on gq.sale_order_line = qo.sale_line_id)
         
         SELECT DISTINCT part_no, description,quantity,weight_net as weight_ea, round(retail_price,2) as retail_price,round(cost_price,2) as cost_price,
          round((quantity-qty_on),2) as qty_on, round((quantity*cost_price),2) as totals, (select sum(weight_net) from tbltam) as weight_total, (select round(sum(quantity*cost_price),2) from tbltam) as sub_total
         FROM tbltam
        """ % orderid
        self.cr.execute(sql)
        res = self.cr.dictfetchall()
        return res

    def _get_total(self, obj):
        orderid = obj.id

        sql = """
         WITH get_qty_untax as(
         SELECT pp.default_code as part_no , sl.name as description, sl.product_uom_qty as quantity, pt.weight_net,sl.product_uom_qty as product_uom_qty, sl.price_unit as price_unit, sl.id as id
          
         FROM sale_order so, sale_order_line sl,product_product pp , product_template pt
         WHERE
         sl.order_id = so.id and sl.product_id = pp.id  and pp.product_tmpl_id = pt.id and sl.order_id = %s
         ),
        get_qty as
         (
         select part_no, gqu.description , gqu.quantity , gqu.weight_net, gqu.price_unit, sm.sale_line_id as sale_order_line, sot.order_line_id as order_line_id, at.id as atid,
         (CASE 
         WHEN price_include = 't' THEN gqu.price_unit
         WHEN price_include = 'f' THEN gqu.price_unit*(1+at.amount)
         ELSE gqu.price_unit
         END) as retail_price,
         (CASE
         WHEN price_include = 'f' THEN gqu.price_unit
         WHEN price_include = 't' THEN gqu.price_unit/(1 + at.amount)
         ELSE  gqu.price_unit
         END) as cost_price
         from get_qty_untax gqu left join sale_order_tax sot on gqu.id = sot.order_line_id
         left join account_tax at on sot.tax_id = at.id
         left join stock_move sm on gqu.id = sm.sale_line_id
         ),
         
         qty_on as(
         SELECT sum(product_qty) as qty_on, sale_line_id,product_id
         FROM stock_move
         WHERE state = 'done'
         GROUP BY sale_line_id,product_id 
         ),
         
         tbltam as(
         SELECT part_no,description, quantity, weight_net, price_unit, sale_order_line, order_line_id, atid, retail_price, cost_price,
         (case
         when qty_on is null then 0
         else qty_on
         end) as qty_on
       
         FROM get_qty gq left join qty_on qo  on gq.sale_order_line = qo.sale_line_id),
         tbltotal as (
         SELECT DISTINCT part_no, description,quantity,weight_net as weight_ea, round(retail_price,2) as retail_price,round(cost_price,2) as cost_price,
          round((quantity-qty_on),2) as qty_on, round((quantity*cost_price),2) as totals, (round((retail_price - cost_price)*quantity,2)) as gst, (round(retail_price * quantity,2)) as incl_gst
         FROM tbltam)

         select sum(totals) as sub_total, sum(gst) as gst, sum(incl_gst) as incl_gst, sum(weight_ea) as total_weight from tbltotal
        """ % orderid
        self.cr.execute(sql)
        res = self.cr.dictfetchall()
        return res

        
    
        
   