using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Kingdee.BOS;
using Kingdee.BOS.App.Data;
using Kingdee.BOS.ServiceFacade.KDServiceFx;
using Kingdee.BOS.WebApi.ServicesStub;
using Newtonsoft.Json;

namespace YHT.K3Erp.WebAPI.ServiceExtend.ServicesStub
{
    /// <summary>
    /// 客户信息服务
    /// </summary>
    [Kingdee.BOS.Util.HotUpdate]
    public class CustomerService : AbstractWebApiBusinessService
    {
        private const int DEFAULT_CREDIT = 500000;

        public CustomerService(KDServiceContext context):base(context)
        {
            
        }

        /// <summary>
        /// 查询客户
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="pagenow"></param>
        /// <param name="percent"></param>
        /// <returns></returns>
        public string QueryCustomer(string pagesize = null, string pagenow = null, string percent = null, string op = ">")
        {
            int PageSize = string.IsNullOrEmpty(pagesize) ? 20 : Convert.ToInt32(pagesize);
            int PageNow = string.IsNullOrEmpty(pagenow) ? 1 : Convert.ToInt32(pagenow);
            DataTable rows = new DataTable();
            DataTable custom_table = new DataTable();
            DataTable ar_table = new DataTable();
            DataTable receivable_table = new DataTable();
            DataTable refund_table = new DataTable();

            rows.Columns.Add("fcustid");
            rows.Columns.Add("fname");
            rows.Columns.Add("credit");
            rows.Columns.Add("arsum");
            rows.Columns.Add("receivables");
            rows.Columns.Add("refunds");
            rows.Columns.Add("arrears");
            rows.Columns.Add("percent");

            //读取数据
            custom_table.Load(CustSql());
            ar_table.Load(CustARSum());
            receivable_table.Load(CustReceivable());
            refund_table.Load(CustRefund());

            for(int i = 0; i < custom_table.Rows.Count; ++i)
            {
                DataRow row = rows.NewRow();
                DataRow _row = custom_table.Rows[i];
                decimal credit = DEFAULT_CREDIT;

                if (!string.IsNullOrEmpty(_row["F_M_CREDIT"].ToString()))
                {
                    credit = Convert.ToDecimal(_row["F_M_CREDIT"]);
                }

                decimal a = findAmount(ar_table, _row["fcustid"].ToString());
                decimal r = findAmount(receivable_table, _row["fcustid"].ToString());
                decimal f = findAmount(refund_table, _row["fcustid"].ToString());
                decimal arrears = a - r + f;

                row["fcustid"] = _row["fcustid"].ToString();
                row["fname"] = _row["FNAME"];
                row["credit"] = credit;
                row["arsum"] = Math.Round(a, 2);
                row["receivables"] = Math.Round(r, 2);
                row["refunds"] = Math.Round(f, 2);
                row["arrears"] = Math.Round(arrears, 2);
                row["percent"] = Math.Round(arrears / credit, 2);
                rows.Rows.Add(row);
            }

            EnumerableRowCollection<DataRow> dataRows = rows.AsEnumerable();
            
            if (!string.IsNullOrEmpty(percent))
            {
                dataRows = dataRows.Where(s => Convert.ToDecimal(s.Field<string>("percent")) >= Convert.ToDecimal(percent));
            }

            int total = dataRows.Count();


            var list = dataRows.Skip((PageNow - 1) * PageSize).Take(PageSize).CopyToDataTable();



            return JsonConvert.SerializeObject(new Dictionary<string, object> { { "state", "success" }, { "total", total }, { "rows", list },{ "pagesize", PageSize }, { "pageNow", PageNow } });
        }

        private decimal findAmount(DataTable rows, string fcustid)
        {
            decimal total = 0;

            for (int i = 0; i < rows.Rows.Count; ++i)
            {
                if (rows.Rows[i]["fcustid"].ToString() == fcustid)
                {
                    total = Convert.ToDecimal(rows.Rows[i]["amount"]);
                    break;
                }
            }


            return total;
        }

        /// <summary>
        /// 读取客户列表
        /// </summary>
        /// <param name="pagenow"></param>
        /// <param name="pagesize"></param>
        /// <returns></returns>
        public string CustomerList(string pagenow = null ,string pagesize = null)
        {
            int total = 0;
            DataTable rows = new DataTable();
            int PageNow = string.IsNullOrEmpty(pagenow) ? 1 : Convert.ToInt32(pagenow);
            int PageSize = string.IsNullOrEmpty(pagesize) ? 10 : Convert.ToInt32(pagesize);
            StringBuilder rsb = new StringBuilder();
            StringBuilder tsb = new StringBuilder();
            // select * from (select C0.FCUSTID, C0.FNAME, C1.F_M_credit, ROw_number() OVER (Order BY C0.FCUSTID) as row_number from T_BD_CUSTOMER_L AS C0 INNER JOIN T_BD_CUSTOMER AS C1 ON C0.FCUSTID = C1.FCUSTID WHERE C1.Fdocumentstatus = 'C') AS t where t.row_number between 10 and 20
            rsb.Append("select * from (");
            rsb.Append("select C0.FCUSTID, C0.FNAME, C1.F_M_credit, ROw_number() OVER (Order BY C0.FCUSTID DESC) as row_number from T_BD_CUSTOMER_L AS C0 ");
            rsb.Append(" INNER JOIN T_BD_CUSTOMER AS C1 ON C0.FCUSTID = C1.FCUSTID ");
            rsb.AppendFormat(" WHERE C1.Fdocumentstatus = 'C') AS t where t.row_number between {0} and {1} ", (PageNow - 1) * PageSize, PageNow * PageSize);
           
            //rsb.AppendFormat("select * from (select *, ROW_number() OVER (ORDER BY FCUSTID) as RowNum from T_BD_CUSTOMER) as t where t.FDOCUMENTSTATUS = 'C' and t.RowNum between {0} and {1} ", (PageNow - 1) * PageSize, PageNow * PageSize );
            tsb.Append("SELECT count(*) AS total FROM T_BD_CUSTOMER where FDOCUMENTSTATUS = 'C'");


            using (IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, rsb.ToString()))
            {
                rows.Load(reader);
                rows.Columns.Add("ARSUM"); // 应收总额
                rows.Columns.Add("RECEIVABLES"); // 收款
                rows.Columns.Add("REFUNS"); // 收款退款
                rows.Columns.Add("ARREARS"); //  应收未收款

                int rowCount = rows.Rows.Count;

                if ( rowCount  > 0)
                {
                    for (int i = 0; i < rowCount; ++i)
                    {
                        string cust_id = rows.Rows[i]["FCUSTID"].ToString();
                        decimal arsum = LoadCustARSum(cust_id);
                        decimal receivables = LoadCustReceivable(cust_id);
                        decimal refunds = LoadCustRefund(cust_id);

                        rows.Rows[i]["ARSUM"] = Math.Round(arsum, 2);
                        rows.Rows[i]["RECEIVABLES"] = Math.Round(receivables,2);
                        rows.Rows[i]["REFUNS"] = Math.Round(refunds);
                        rows.Rows[i]["ARREARS"] = Math.Round(arsum - receivables + refunds); // 应收总额 - 收款 + 退款

                        if (!string.IsNullOrEmpty(rows.Rows[i]["F_M_CREDIT"].ToString()))
                        {
                            rows.Rows[i]["F_M_CREDIT"] = Math.Round(Convert.ToDecimal(rows.Rows[i]["F_M_CREDIT"]));
                        }
                        else
                        {
                            rows.Rows[i]["F_M_CREDIT"] = DEFAULT_CREDIT;
                        }
                        
                    }
                }
            }

            using (IDataReader totalReader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, tsb.ToString()))
            {
                if (totalReader.Read())
                {
                    total = Convert.ToInt32(totalReader["total"]);
                }
            }

            Dictionary<string, object> data = new Dictionary<string, object>() { { "rows", rows }, { "total", total }, { "state", "success" } };
           
            return JsonConvert.SerializeObject(data);
        }


        /// <summary>
        /// 更新客户的信用额度
        /// </summary>
        /// <param name="cust_id"></param>
        /// <param name="credit"></param>
        /// <returns></returns>
        public string UpdateCustomerCredit(string cust_id, string credit)
        {
            decimal _credit = Convert.ToDecimal(credit);
            Dictionary<string, string> response = new Dictionary<string, string>();

            if (!(_credit < 0) && !string.IsNullOrEmpty(cust_id))
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("UPDATE T_BD_CUSTOMER SET F_M_credit = {0} WHERE FCUSTID = {1}", _credit, cust_id);

                int state = DBUtils.Execute(this.KDContext.Session.AppContext, sb.ToString());
                response["state"] = "success";
            }
            else
            {
                response["state"] = "error";
                response["errmsg"] = "请输入有效的额度或者选择客户";
               
            }

            return JsonConvert.SerializeObject(response);
        }


        

        /// <summary>
        /// 读取客户应收款汇总
        /// </summary>
        /// <param name="cust_id"></param>
        /// <returns></returns>
        public decimal LoadCustARSum(string cust_id)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SELECT sum(FallAmountFor) as amount from t_AR_receivable WHERE fcustomerID = {0} and Fdocumentstatus = 'C' ", cust_id);

            return LoadAmount(sb.ToString());
        }

       

        /// <summary>
        /// 读取客户收款
        /// </summary>
        /// <param name="cust_id"></param>
        /// <returns></returns>
        public decimal LoadCustReceivable(string cust_id)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SELECT SUM(FRECEIVEAMOUNT) AS amount FROM T_AR_RECEIVEBILL WHERE fcontactunit = {0} and Fdocumentstatus = 'C'", cust_id);
            return LoadAmount(sb.ToString());
        }


        /// <summary>
        /// 读取收款退款
        /// </summary>
        /// <param name="cust_id"></param>
        /// <returns></returns>
        public decimal LoadCustRefund(string cust_id)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SELECT SUM(FREFUNDTOTALAMOUNTFOR) AS amount FROM T_AR_REFUNDBILL WHERE  fcontactunit = {0} and Fdocumentstatus = 'C'", cust_id);
            return LoadAmount(sb.ToString());
        }


        /// <summary>
        /// 执行sql读取金额
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        private decimal LoadAmount(string sql)
        {
            decimal total = 0;
            using (IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sql))
            {
                if (reader.Read())
                {
                    string amount = reader["amount"].ToString();

                    if (!string.IsNullOrEmpty(amount))
                    {
                        total = Convert.ToDecimal(amount);
                    }
                }
            }

            return total;
        }

        /// <summary>
        /// 读取全部客户的sql
        /// </summary>
        /// <returns></returns>
        private IDataReader CustSql()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("select * from (");
            sb.Append("select C0.FCUSTID as fcustid, C0.FNAME, C1.F_M_credit, ROw_number() OVER (Order BY C0.FCUSTID DESC) as row_number from T_BD_CUSTOMER_L AS C0 ");
            sb.Append(" INNER JOIN T_BD_CUSTOMER AS C1 ON C0.FCUSTID = C1.FCUSTID ");
            sb.AppendFormat(" WHERE C1.Fdocumentstatus = 'C') AS t where t.row_number > 0 ");

            IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sb.ToString());

            return reader;
        }

        /// <summary>
        /// 读取每个客户应收的sql
        /// </summary>
        /// <returns></returns>
        private IDataReader CustARSum()
        {
           
            string sql = "SELECT fcustomerID as fcustid, sum(FallAmountFor) as amount from t_AR_receivable WHERE  Fdocumentstatus = 'C' GROUP BY fcustomerID ";
            IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sql);

            return reader;
        }

        /// <summary>
        /// 读取每个客户收款的sql
        /// </summary>
        /// <returns></returns>
        private IDataReader CustReceivable()
        {
            
            string sql = "SELECT fcontactunit as fcustid, SUM(FRECEIVEAMOUNT) AS amount FROM T_AR_RECEIVEBILL WHERE Fdocumentstatus = 'C' GROUP BY fcontactunit";
            IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sql);

            return reader;
        }

        /// <summary>
        /// 读取每个客户退款的sql
        /// </summary>
        /// <returns></returns>
        private IDataReader CustRefund()
        {
            string sql = "SELECT fcontactunit as fcustid, SUM(FREFUNDTOTALAMOUNTFOR) AS amount FROM T_AR_REFUNDBILL WHERE   Fdocumentstatus = 'C' group by fcontactunit";

            IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sql);

            return reader;
        }

    }
}
