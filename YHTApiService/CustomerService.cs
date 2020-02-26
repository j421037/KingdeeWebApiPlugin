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
        private const int DEFAULT_CREDIT = 100000;

        private const string TABLE_NAME = "YHT_Settings";

        public CustomerService(KDServiceContext context):base(context)
        {
            
        }

        /// <summary>
        /// 查询客户数据列表
        /// </summary>
        /// <param name="pagesize"></param>
        /// <param name="pagenow"></param>
        /// <param name="cust_id"></param>
        /// <param name="percent"></param>
        /// <param name="op"></param>
        /// <returns></returns>
        public string QueryCustomer(string pagesize = null, string pagenow = null, string cust_id = null, string percent = null, string op = "gt")
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
            rows.Columns.Add("percent_str");

            //读取数据
            custom_table.Load(CustSql(cust_id));
            ar_table.Load(CustARSum(cust_id));
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
                row["percent_str"] = $"{row["percent"]:P}";
                rows.Rows.Add(row);
            }

            EnumerableRowCollection<DataRow> dataRows = rows.AsEnumerable();
          
            if (!string.IsNullOrEmpty(percent) && string.IsNullOrEmpty(cust_id))
            {
                decimal _p = Convert.ToDecimal(percent);
                string _op = string.IsNullOrEmpty(op) ? "gt" : op;

                dataRows = dataRows.Where(s => {
                    if (_op.ToLower().Equals("gt"))
                    {
                        return Convert.ToDecimal(s.Field<string>("percent")) >= _p;
                    }
                    else
                    {
                        return Convert.ToDecimal(s.Field<string>("percent")) <= _p;
                    }
                    
                });
            }

            int total = dataRows.Count();


            var list = dataRows.Skip((PageNow - 1) * PageSize).Take(PageSize).CopyToDataTable();



            return JsonConvert.SerializeObject(new Dictionary<string, object> { { "state", "success" }, { "total", total }, { "rows", list },{ "pagesize", PageSize }, { "pageNow", PageNow } });
        }

        /// <summary>
        /// 遍历数据表
        /// </summary>
        /// <param name="rows"></param>
        /// <param name="fcustid"></param>
        /// <returns></returns>
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
        /// 检索客户
        /// </summary>
        /// <param name="fname"></param>
        /// <returns></returns>
        public string SearchCust(string fname = null)
        {
            DataTable dt = new DataTable();

            if (!string.IsNullOrEmpty(fname))
            {
                
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("SELECT fcustid, fname FROM T_BD_CUSTOMER_L WHERE fname LIKE '%{0}%'", fname);

                using (IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sb.ToString()))
                {
                    dt.Load(reader);
                }
            }

            return JsonConvert.SerializeObject(new Dictionary<string, object>() { { "state", "success" }, { "rows", dt } });
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
        /// 更新公司额度
        /// </summary>
        /// <param name="total"></param>
        /// <returns></returns>
        public string UpdateCompanyTotal(string total)
        {
            Dictionary<string, string> response = new Dictionary<string, string>();
            
            try
            {
                bool state = tableExists(TABLE_NAME);
                response["tableExists"] = state.ToString();
                if (!state)
                {
                    createTable(TABLE_NAME);
                }

                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("if not exists (select * from {0} where p_key = 'company_total') INSERT INTO {1}(p_key, p_value) VALUES ('company_total', {2}) ", TABLE_NAME, TABLE_NAME, total);
                sb.AppendFormat("else UPDATE {0} SET p_value = {1} where p_key = 'company_total'", TABLE_NAME, total);

                DBUtils.Execute(this.KDContext.Session.AppContext, sb.ToString());

                response["state"] = "success";
            }
            catch (Exception ex)
            {
                response["errmsg"] = ex.Message;
                response["trace"] = ex.StackTrace;
                response["state"] = "error";
            }

           

            return JsonConvert.SerializeObject(response);
        }

        /// <summary>
        /// 读取公司总额度
        /// </summary>
        /// <returns></returns>
        public string LoadCompanyTotal()
        {
            Dictionary<string, object> response = new Dictionary<string, object>();

            try
            {
                if (!tableExists(TABLE_NAME))
                {
                    createTable(TABLE_NAME);
                }

                string total = "";

                string sql = string.Format("select * from {0} where p_key = 'company_total'", TABLE_NAME);

                Dictionary<string, string> data = new Dictionary<string, string>();

                IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sql);

                if (reader.Read())
                {
                    total = reader["p_value"].ToString();
                }

                response["data"] = new Dictionary<string, string>() { { "total", total } };

            }
            catch (Exception ex)
            {
                response["errmsg"] = ex.Message;
                response["trace"] = ex.StackTrace;
                response["state"] = "error";
            }

            return JsonConvert.SerializeObject(response);
        }
       
        /// <summary>
        /// 读取全部客户的sql
        /// </summary>
        /// <returns></returns>
        private IDataReader CustSql(string cust_id = null)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("select * from (");
            sb.Append("select C0.FCUSTID as fcustid, C0.FNAME, C1.F_M_credit, ROw_number() OVER (Order BY C0.FCUSTID DESC) as row_number from T_BD_CUSTOMER_L AS C0 ");
            sb.Append(" INNER JOIN T_BD_CUSTOMER AS C1 ON C0.FCUSTID = C1.FCUSTID ");
            sb.AppendFormat(" WHERE C1.Fdocumentstatus = 'C') AS t where t.row_number > 0 ");

            if (!string.IsNullOrEmpty(cust_id))
            {
                sb.AppendFormat(" AND t.fcustid = {0} ", cust_id);
            }

            IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sb.ToString());

            return reader;
        }

        /// <summary>
        /// 读取每个客户应收的sql
        /// </summary>
        /// <returns></returns>
        private IDataReader CustARSum(string cust_id = null)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT fcustomerID as fcustid, sum(FallAmountFor) as amount from t_AR_receivable WHERE  Fdocumentstatus = 'C' ");

            if (!string.IsNullOrEmpty(cust_id))
            {
                sb.AppendFormat(" AND fcustomerID = {0} ", cust_id);
            }

            sb.Append(" GROUP BY fcustomerID ");
            //string sql = "SELECT fcustomerID as fcustid, sum(FallAmountFor) as amount from t_AR_receivable WHERE  Fdocumentstatus = 'C' GROUP BY fcustomerID ";
            IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sb.ToString());

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

        /// <summary>
        /// 检查表是否存在
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private bool tableExists(string name)
        {
            string sql = string.Format("select * from sysobjects where type='U' and name='{0}'", name);
            // int state_ = DBUtils.ExecuteScalar<int>(this.KDContext.Session.AppContext, sql, 0 );
            IDataReader reader = DBUtils.ExecuteReader(this.KDContext.Session.AppContext, sql, 0);

            return reader.Read();

        }

        /// <summary>
        /// c创建表
        /// </summary>
        /// <param name="name"></param>
        private bool createTable(string name)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("create table {0}(  p_key VARCHAR(255), p_value VARCHAR(255) default null)", name);
            int state =  DBUtils.Execute(this.KDContext.Session.AppContext, sb.ToString());

            if (Convert.ToBoolean(state))
            {
                string sql = string.Format("INSERT INTO {0} (p_key) values (`company_total`)", name);
                state = DBUtils.Execute(this.KDContext.Session.AppContext, sql);

                return Convert.ToBoolean(state);
            }
            return Convert.ToBoolean(state);
        }

    }
}
