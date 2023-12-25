using MySql.Data.MySqlClient;
using NCrontab;
using NL_Results_Loader.Class;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace NL_Results_Loader
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
            LoadData();
            timer1.Enabled = true;
        }

        private void LoadData()
        {
            MySqlConnection mysqlCon = new MySqlConnection(ConfigurationManager.ConnectionStrings["connection"].ConnectionString);
            mysqlCon.Open();

            MySqlDataAdapter MyDA = new MySqlDataAdapter();
            string sqlSelectAll = "SELECT * FROM nl_stat.scheduler_Loader;";
            MyDA.SelectCommand = new MySqlCommand(sqlSelectAll, mysqlCon);

            DataTable table = new DataTable();
            MyDA.Fill(table);

            BindingSource bSource = new BindingSource();
            bSource.DataSource = table;


            dataGridView1.DataSource = bSource;
        }

        private void runNowToolStripMenuItem_Click(object sender, EventArgs e)
        {
            int ID = (int)dataGridView1.SelectedRows[0].Cells[0].Value;
            RunSchedule(ID);
        }

        private void dataGridView1_CellMouseClick(object sender, DataGridViewCellMouseEventArgs e)
        {
            LoadData();
            if (e.Button == System.Windows.Forms.MouseButtons.Right)
            {
                dataGridView1.Rows[e.RowIndex].Selected = true;
                contextMenuStrip1.Show(MousePosition);
                
            }
        }

        void RunSchedule(int ID)
        {
            try
            {
                var scheduler = new DatabaseManagement().GetAllSchedules();
                Scheduler schedule = new DatabaseManagement().GetScheduleByID(ID);

                schedule.LastRunTime = DateTime.Now;
                if (!schedule.Schedule.StartsWith("#"))
                {
                    schedule.LastResult = "Running...";


                    bool bUpdateDB = new DatabaseManagement().UpdateScheduler(schedule);

                    schedule.LastResult = "";

                    int iCurrDraw = 0;
                    int.TryParse(schedule.Counter, out iCurrDraw);
                    string sGame = schedule.Parameters;
                    ApiDetails apiDetails = new ApiDetails();
                    apiDetails = new DatabaseManagement().GetApiDetailsByName(schedule.Script);

                    apiDetails.RequestURL = apiDetails.RequestURL.Replace("{gamename}", sGame).Replace("{gameid}", iCurrDraw.ToString());

                    ApiDetails apiNextDraw = new ApiDetails();
                    apiNextDraw = new DatabaseManagement().GetApiDetailsByName(schedule.Script);
                    int iNextDraw = iCurrDraw + 1;
                    apiNextDraw.RequestURL = apiNextDraw.RequestURL.Replace("{gamename}", sGame).Replace("{gameid}", iNextDraw.ToString());

                    string json = new DatabaseManagement().APIWebRequest(apiDetails);
                    string jsonNext = new DatabaseManagement().APIWebRequest(apiNextDraw);
                    DrawJSON gameDraw = null; DrawJSON nextGameDraw = null;

                    if (json != "")
                    {
                        gameDraw = JsonSerializer.Deserialize<DrawJSON>(json);

                    }

                    if (gameDraw.status == "PAYABLE")
                    {

                        if (jsonNext != "")
                        {
                            nextGameDraw = JsonSerializer.Deserialize<DrawJSON>(jsonNext);
                        }

                        bool bInsert = false;

                        switch (schedule.Name)
                        {
                            case "FastKeno":
                                bInsert = new DatabaseManagement().InsertRecordKeno(gameDraw, nextGameDraw);
                                break;
                        }

                        if (bInsert)
                        {
                            schedule.Counter = iNextDraw.ToString();
                            var vCronTab = CrontabSchedule.Parse(schedule.Schedule);
                            DateTime dtNextSchedule = vCronTab.GetNextOccurrence(DateTime.Now);
                            schedule.NextRunTime = dtNextSchedule;
                            schedule.LastRunTime = DateTime.Now;
                            schedule.LastResult = "1 record processed.";
                            bUpdateDB = new DatabaseManagement().UpdateScheduler(schedule);
                        }
                    }
                }
                else
                {
                    schedule.NextRunTime = null;
                }
            }
            catch(Exception ex)
            { }
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            List<Scheduler> schedules = new DatabaseManagement().GetAllSchedules();

            foreach (var schedule in schedules)
            {
                if (DateTime.Now > schedule.NextRunTime)
                {
                    RunSchedule(schedule.ID);
                }
            }
        }
    }
}
