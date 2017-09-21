using BridgeCore.events;
using BridgeServerMySQL.models;
using BrokerCore;
using MasterCore;
using MasterCore.controllers;
using MasterCore.events;
using MasterCore.models;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Windows.Forms;

namespace BridgeServerMySQL
{
    public partial class MainForm : Form
    {


        BrokerCoreService brokerService;
        MasterCoreService masterService;

        Thread brokerServiceThread;
        Thread masterServiceThread;

        bool exitBrokerThread = false;
        bool exitMasterThread = false;


        static string UserID = Guid.NewGuid().ToString();
        //Dictionary<int,User> _users;
        Dictionary<string, Bridge> _bridges;
        Dictionary<string, Device> _devices;
        Dictionary<int, TextMessage> _messages;
        Dictionary<Guid, Location> _locations;
        Dictionary<int, BuzzerCall> _buzzers;
        Setting setting;

        //// every 5 second report device location once to master service
        //int _locationProcessPeriod = 5;
        //// every 10 second broker self report once to master service
        //int _selfReportPeriod = 10;
        //// if bridge report greater than 15 seconds, decide it lost
        //int _bridgeLostThreshold = 15;
        //// if device report greater than 7 seconds, decide it lost
        //int _deviceLostThreshold = 10;

        bool serviceRunning = false;

        ViewDataBaseForm view_from = null;


        int periodCounter = 0;

        //private List<MySqlConnection> connPool;
        //private Mutex _connMutex;

        public MainForm()
        {
            InitializeComponent();
        }

        private void MainForm_Load(object sender, EventArgs e)
        {
            Trace.Listeners.Add(new Log4NetTraceListener());

            Initial();

        }


        private IniFile GetIniFile()
        {
            string iniPath = string.Format("{0}\\setting.ini", Application.StartupPath);

            if (!File.Exists(iniPath))
            {
                StreamWriter sw = File.CreateText(iniPath);
                sw.Close();
            }


            IniFile ini = new IniFile(iniPath);

            return ini;
        }

        //private void LoadIni()
        //{
        //    IniFile ini = GetIniFile();

        //    // load setting
        //    string strLocationProcessPeriod = ini.IniReadValue("Setting", "LocationProcessPeriod");
        //    if (strLocationProcessPeriod.Equals(""))
        //        ini.IniWriteValue("Setting", "LocationProcessPeriod", _locationProcessPeriod.ToString());
        //    else
        //        _locationProcessPeriod = int.Parse(strLocationProcessPeriod);

        //    string strSelfReportPeriod = ini.IniReadValue("Setting", "SelfReportPeriod");
        //    if (strSelfReportPeriod.Equals(""))
        //        ini.IniWriteValue("Setting", "SelfReportPeriod", _selfReportPeriod.ToString());
        //    else
        //        _selfReportPeriod = int.Parse(strSelfReportPeriod);

        //    string strBridgeLostThreshold = ini.IniReadValue("Setting", "BridgeLostThreshold");
        //    if (strBridgeLostThreshold.Equals(""))
        //        ini.IniWriteValue("Setting", "BridgeLostThreshold", _bridgeLostThreshold.ToString());
        //    else
        //        _bridgeLostThreshold = int.Parse(strBridgeLostThreshold);

        //    string strDeviceLostThreshold = ini.IniReadValue("Setting", "DeviceLostThreshold");
        //    if (strDeviceLostThreshold.Equals(""))
        //        ini.IniWriteValue("Setting", "DeviceLostThreshold", _deviceLostThreshold.ToString());
        //    else
        //        _deviceLostThreshold = int.Parse(strDeviceLostThreshold);
        //}

        private void Initial()
        {
            //Trace.WriteLine(string.Format("UtcNow={0}", Global.UnixTimestampFromDateTime(DateTime.UtcNow)));
            //Trace.WriteLine(string.Format("Now={0}", Global.UnixTimestampFromDateTime(DateTime.Now)));

            string local_ip = GetLocalIPAddress();
            this.Text = string.Format("Promos Bridge Server MSSQL {0} - [ {1} ]", Global.Version, local_ip);

            string path = string.Format("{0}\\{1}", Application.StartupPath, "default.json");
            //if (MSSQL.TestConnection(path))
            //{
            MSSQL.CreateInstance(path);
            //ConnectionPool.CreateInstance(MySQL.Instance().ConnString);

            LoadData();

            lblConnString.Text = MSSQL.Instance().ConnString;
            //}
        }

        //private MySqlConnection GetConnection()
        //{
        //    MySqlConnection conn = null;
        //    _connMutex.WaitOne();
        //    foreach (MySqlConnection _conn in connPool)
        //    {
        //        if (_conn.State == ConnectionState.Open)
        //        {
        //            conn = _conn;
        //            break;
        //        }
        //    }
        //    _connMutex.ReleaseMutex();

        //    return conn;
        //}

        private void LoadData()
        {
            //_users = new Dictionary<int, User>();
            _bridges = new Dictionary<string, Bridge>();
            _devices = new Dictionary<string, Device>();
            _messages = new Dictionary<int, TextMessage>();
            _locations = new Dictionary<Guid, models.Location>();

            LoadSetting();
            //LoadUsers();
            LoadLocations();
            LoadBridges();
            LoadDevices();

            //LoadIni();


        }


        private void LoadSetting()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SELECT * FROM dbo.Settings");
            DataTable dt = MSSQL.Instance().GetMyDataTable(sb.ToString());
            DataRow row = dt.Rows[0];

            setting = new Setting(row);
        }

        //private void LoadUsers()
        //{
        //    DataTable dt = MSSQL.Instance().GetMyDataTable(string.Format("SELECT id,username FROM {0}.users",
        //            MSSQL.Instance().Database));

        //    foreach (DataRow row in dt.Rows)
        //    {
        //        //User user = new User(row);
        //        //_users.Add(user.ID, user);

        //        LoadBridges();
        //        LoadDevices();
        //    }
        //}

        private void LoadBridges()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SELECT * FROM dbo.Bridges");
            DataTable dt = MSSQL.Instance().GetMyDataTable(sb.ToString());

            _bridges.Clear();
            foreach (DataRow row in dt.Rows)
            {
                Bridge bridge = new Bridge(row);
                _bridges.Add(bridge.UUID, bridge);
            }
        }

        private void LoadDevices()
        {
            DataTable dt = MSSQL.Instance().GetMyDataTable("SELECT * FROM dbo.Devices");

            _devices.Clear();
            foreach (DataRow row in dt.Rows)
            {
                Device device = new Device(row);
                _devices.Add(device.UUID, device);
            }
        }

        private void LoadLocations()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SELECT * FROM dbo.Locations");
            DataTable dt = MSSQL.Instance().GetMyDataTable(sb.ToString());

            _locations.Clear();
            foreach (DataRow row in dt.Rows)
            {
                Location location = new Location(row);
                _locations.Add(location.ID, location);
            }
        }

        private void AppendLog(string message)
        {
            this.Invoke((MethodInvoker)delegate
            {
                message = string.Format("{0} - {1}", DateTime.Now.ToString("yyyy/MM/dd hh:mm:ss"), message);
                Trace.WriteLine(message);
                lstLog.Items.Add(message);
                int visibleItems = lstLog.ClientSize.Height / lstLog.ItemHeight;
                lstLog.TopIndex = Math.Max(lstLog.Items.Count - visibleItems + 1, 0);
            });
        }

        public static DataGridViewColumn TextBoxColumn(string text, int width)
        {
            DataGridViewColumn col = new DataGridViewTextBoxColumn();
            col.SortMode = DataGridViewColumnSortMode.NotSortable;
            col.Name = col.HeaderText = text;
            col.Width = width;
            return col;
        }

        public static DataGridViewColumn TextBoxColumn(string text)
        {
            DataGridViewColumn col = new DataGridViewTextBoxColumn();
            col.SortMode = DataGridViewColumnSortMode.NotSortable;
            col.Name = col.HeaderText = text;

            return col;
        }


        private void dataClear()
        {
            foreach (KeyValuePair<string, Bridge> item in _bridges)
            {
                item.Value.Online = false;
            }
            foreach (KeyValuePair<string, Device> item in _devices)
            {
                item.Value.Online = false;
                item.Value.BridgeUUID = "";
            }

            MSSQL.Instance().MySqlExcute("UPDATE Bridges SET Online=0; UPDATE Devices SET Online=0");
        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            dataClear();

            lstLog.Items.Clear();


            brokerServiceThread = new Thread(BrokerThread);
            brokerServiceThread.Start();


            masterServiceThread = new Thread(MasterThread);
            masterServiceThread.Start();


        }


        public static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }
            throw new Exception("Local IP Address Not Found!");
        }

        private void BrokerThread()
        {
            string local_ip = GetLocalIPAddress();
            Console.WriteLine("Local IP " + local_ip);

            Console.WriteLine("Broker Start.");

            exitBrokerThread = false;
            while (!exitBrokerThread)
            {
                //LocationDeterminateRule rule = LocationDeterminateRule.CreateInstance(LocationTrustDiff, LocationTrustCycle, LocationGrayregionEnable, LocationGrayregionCycle);
                LocationDeterminateRule rule = LocationDeterminateRule.CreateInstance(setting.LocationTrustDiff, setting.LocationTrustCycle, setting.LocationGrayregionEnable, setting.LocationGrayregionCycle);

                brokerService = new BrokerCoreService("127.0.0.1", setting.MQTTPort, setting.BrokerPort, local_ip, setting.BrokerSelfReportPeriod, setting.DeviceLostThres, rule);
                // hook all raw messge used to do indoor location
                brokerService.RawDataChanged += BrokerService_RawDataChanged;

                while (!exitBrokerThread)
                {
                    if (brokerService.Start()) break;
                }

                Console.WriteLine("Broker Service Running.");

                while (brokerService.Running())
                {
                    Thread.Sleep(1000);
                }

                Console.WriteLine("Broker Service Terminate.");
            }
        }
        Boolean isRawDataChangedLog = true;
        //System.Collections.ArrayList ArrayList = new System.Collections.ArrayList();
        //MSSQL test = new MSSQL(@".\SQLEXPRESS", "blutest", "sa", "promos");
        private void BrokerService_RawDataChanged(object sender, RawEventArgs e)
        {


            // hook all raw messge used to do indoor location
            if (isRawDataChangedLog)
            {
                Trace.Write("RAW - " + e.Raw.message);
                List<string> newLines = new List<string>();
                string row = String.Format("{0},{1}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"), e.Raw.message);
                newLines.Add(row);
                File.AppendAllLines(@".\LogFiles\" + e.Raw.DeviceUUID + ".txt", newLines);//每個device的RSSI
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("INSERT INTO dbo.test ");
                sb.AppendFormat("(UUID,SNR) VALUES ");
                sb.AppendFormat("('{0}','{1}')",
                    e.Raw.DeviceUUID, e.Raw.SNR);
                //test.MySqlExcute(sb.ToString());
            }
        }

        private void StopBroker()
        {
            exitBrokerThread = true;
            //brokerService.Terminate();
        }

        private int GetDeviceType(Device device)
        {
            if (device.Type == 0)
                return DeviceModel.DEVICE_TYPE_TAG;
            if (device.Type == 1)
                return DeviceModel.DEVICE_TYPE_BAND;

            return DeviceModel.DEVICE_TYPE_NONE;
        }

        private void MasterThread()
        {
            Console.WriteLine("Master Start.");

            masterService = new MasterCoreService(setting.MQTTPort, setting.BrokerPort, setting.BrokerSelfReportPeriod, setting.BrokerLostThres, setting.DeviceLostThres, setting.BridgeLostThres);

            // input data into master service 

            // add one user

            masterService.DataBase.AddUser(UserID);


            // add bridges
            foreach (KeyValuePair<string, Bridge> item in _bridges)
            {
                Bridge bridge = item.Value;
                bridge.Online = false;

                BridgeConfigModel config = new BridgeConfigModel();
                config.LocationCalPeriod = bridge.LocationCalPeriod;
                config.SelfReportPeriod = bridge.SelfReportPeriod;
                config.ScanPeriod = bridge.ScanPeriod;
                config.ReportPeriod = bridge.ReportPeriod;
                config.ScanSignalLimit = bridge.ScanSignalLimit;
                config.MessageSignalLimit = bridge.MessageSignalLimit;
                config.EnableTagFilter = bridge.EnableTagFilter;
                config.EnableBandFilter = bridge.EnableBandFilter;
                config.RSSI_Compensate = bridge.RSSI_Compensate;
                config.ProximityThres = bridge.ProximityThres;
                masterService.DataBase.AddBridge(UserID, bridge.UUID, config);
            }

            // add devices
            foreach (KeyValuePair<string, Device> item in _devices)
            {
                Device device = item.Value;
                device.Online = false;
                device.BridgeUUID = "";
                masterService.DataBase.AddDevice(UserID, device.UUID, GetDeviceType(device));
            }

            masterService.ServiceStart += MasterService_ServiceStart;
            masterService.ServiceTerminate += MasterService_ServiceTerminate;
            masterService.DeviceOnlined += MasterService_DeviceOnlined;
            masterService.DeviceOfflined += MasterService_DeviceOfflined;
            masterService.BridgeOnlined += MasterService_BridgeOnlined;
            masterService.BridgeOfflined += MasterService_BridgeOfflined;
            masterService.BridgeSelfReport += MasterService_BridgeSelfReport;
            masterService.LocationReported += MasterService_LocationReported;
            masterService.MessageStatusChanged += MasterService_MessageStatusChanged;
            masterService.BuzzerStatusChanged += MasterService_BuzzerStatusChanged;
            masterService.Start(setting.ServiceDelayStart);
            Console.WriteLine("Master Service Running.");

            exitMasterThread = false;
            while (!exitMasterThread)
            {
                while (masterService.Running())
                {
                    if (exitMasterThread)
                        masterService.Terminate();
                    Thread.Sleep(1000);
                }
            }

            Console.WriteLine("Master Service Terminate.");
        }

        private void MasterService_ServiceTerminate(object sender, EventArgs e)
        {
            // remove event hook
            brokerService.RawDataChanged -= BrokerService_RawDataChanged;

            masterService.DeviceOnlined -= MasterService_DeviceOnlined;
            masterService.DeviceOfflined -= MasterService_DeviceOfflined;
            masterService.BridgeOnlined -= MasterService_BridgeOnlined;
            masterService.BridgeOfflined -= MasterService_BridgeOfflined;
            masterService.LocationReported -= MasterService_LocationReported;
            masterService.MessageStatusChanged -= MasterService_MessageStatusChanged;
            masterService.BuzzerStatusChanged -= MasterService_BuzzerStatusChanged;

            this.Invoke((MethodInvoker)delegate
            {


                //btnBridgeAdd.Visible = true;
                //if (_bridges.Count > 0)
                //    btnBridgeDel.Visible = true;

                //btnDeviceAdd.Visible = true;
                //if (_devices.Count > 0)
                //    btnDeviceDel.Visible = true;

                serviceRunning = false;

                btnStop.Visible = false;
                btnStart.Visible = true;

                timerPeriod.Stop();
                timerMessage.Stop();
                timerTimeOutMessage.Stop();
            });
        }

        private void MasterService_ServiceStart(object sender, EventArgs e)
        {
            this.Invoke((MethodInvoker)delegate
            {
                //btnBridgeAdd.Visible = false;
                //btnBridgeDel.Visible = false;
                //btnBridgeSave.Visible = false;

                //btnDeviceAdd.Visible = false;
                //btnDeviceDel.Visible = false;
                //btnDeviceSave.Visible = false;

                serviceRunning = true;

                btnStart.Visible = false;
                btnStop.Visible = true;

                timerPeriod.Start();
                timerMessage.Start();
                timerTimeOutMessage.Start();
            });
        }

        private void MasterService_BridgeOfflined(object sender, BaseEventArgs e)
        {
            Bridge bridge = _bridges[e.UUID];
            bridge.Online = false;

            BridgeUpdate(bridge);
            //this.Invoke((MethodInvoker)delegate {
            //    dgBridge_Load();
            //});

            AppendLog(string.Format("Bridge {0} Offline", e.UUID));
        }

        private void MasterService_BridgeOnlined(object sender, BridgeOnlineEventArgs e)
        {
            Bridge bridge = _bridges[e.UUID];
            bridge.Online = true;

            BridgeUpdate(bridge);
            //this.Invoke((MethodInvoker)delegate {
            //    dgBridge_Load();
            //});

            AppendLog(string.Format("Bridge {0} Online", e.UUID));
        }
        /// <summary>
        /// report UUID
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e">Bridge Report</param>
        private void MasterService_BridgeSelfReport(object sender, BaseEventArgs e)
        {
            Bridge bridge = _bridges[e.UUID];
            bridge.Online = true;
            BridgeUpdate(bridge);
        }
        /// <summary>
        /// Device Off line EXIT location 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e">Device Off line</param>
        private void MasterService_DeviceOfflined(object sender, BaseEventArgs e)
        {
            Device device = _devices[e.UUID];
            Guid locationID = device.LocationID;

            if (device.BridgeUUID != null && !device.BridgeUUID.Equals(""))
            {
                Bridge bridge = _bridges[device.BridgeUUID];
                LocationExitEvent("EXIT", locationID, device);
            }
            device.Online = false;
            device.BridgeUUID = "";
            device.LocationID = Guid.Empty;
            DeviceUpdate(device);

            AppendLog(string.Format("Device {0} Offline", e.UUID));
        }
        /// <summary>
        /// When Device Online record Device location and Device Onlined
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e">Device Online</param>
        private void MasterService_DeviceOnlined(object sender, BaseEventArgs e)
        {
            //DeviceModel deviceModel = masterService.FindDevice
            Device device = _devices[e.UUID];
            device.Online = true;
            //device.BridgeUUID = 
            //this.Invoke((MethodInvoker)delegate {
            //    dgDevice_Load();
            //});
            DeviceUpdate(device);

            //DeviceUpdate(device);

            AppendLog(string.Format("Device {0} Online", e.UUID));
        }

        /// <summary>
        /// record Message Status
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e">
        /// e.Status INIT = 0 SENTED = 1 HAVE_READ = 2 TIMEOUT = 3
        /// </param>
        private void MasterService_MessageStatusChanged(object sender, MessageEventArgs e)
        {
            int id = e.ID;
            int status = e.Status;

            //if (!_messages.ContainsKey(id)) return;

            //TextMessage message = _messages[id];
            //message.Status = status;
            AppendLog(string.Format("{0},{1}", id, status));
            MessageUpdate(id, status);

     


            //this.Invoke((MethodInvoker)delegate {
            //    dgMessage_Load();
            //});

        }

        private void MasterService_BuzzerStatusChanged(object sender, BuzzerEventArgs e)
        {
            int id = e.ID;
            int status = e.Status;

            MessageUpdate(id, status);
        }

        private string GetLocationID(Guid bridgeID)
        {
            string sql = string.Format("SELECT ID from Locations as l, Bridges as b WHERE b.Location_ID = l.ID AND b.ID = '{0}'", bridgeID.ToString());
            var id = MSSQL.Instance().MySqlExcuteScalar(sql);

            return id.ToString();
        }
        /// <summary>
        /// Deivce Location Reported
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MasterService_LocationReported(object sender, LocationEventArgs e)
        {
            string deviceUUID = e.DeviceUUID;

            Device device = _devices[deviceUUID];

            Bridge bridge = _bridges[e.BridgeUUID];

            Guid oldLocationID = Guid.Empty;
            string oldBridgeUUID = device.BridgeUUID;
            int oldBridgeChannel = device.BridgeChannel;

            if (device.LocationID != null && !device.LocationID.Equals(Guid.Empty))
                oldLocationID = device.LocationID;

            //string oldBridgeUUID = device.BridgeUUID;

            device.BridgeUUID = e.BridgeUUID;
            device.BridgeChannel = e.BridgeChannel;

            //device Channel is 天線
            if (device.BridgeChannel == 0)
                device.LocationID = bridge.LocationID;
            else
                device.LocationID = bridge.LocationID1;

            Guid locationID = device.LocationID;
            device.RSSI = e.Raw.SNR;
            device.Battery = e.Raw.Battery;
            device.Last_Bridge = bridge.ID;
            try
            {
                DeviceUpdate(device);
                //Device Location change
                if (!oldLocationID.Equals(locationID))
                {
                    LocationEnterEvent("ENTER", locationID, oldLocationID, device);
                    //DeviceUpdateLastEnter(device);
                    AppendLog(string.Format("Device {0} Changed to {1} Channel {2}", device.UUID, device.BridgeUUID, device.BridgeChannel));
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.Message);
            }
        }
        /// <summary>
        /// Location Exit Event Update DB
        /// </summary>
        /// <param name="type"></param>
        /// <param name="location1"></param>
        /// <param name="device"></param>
        private void LocationExitEvent(string type, Guid location1, Device device)
        {
            var obj = MSSQL.Instance().MySqlExcuteScalar(
                string.Format("SELECT TOP 1 ID FROM dbo.Materials as m, dbo.MaterialDevice as r WHERE r.MaterialId = m.ID AND r.DeviceId ='{0}'AND m.enabled ='True'", device.ID));
            //string.Format("SELECT TOP 1 ID FROM dbo.Materials as m, dbo.MaterialDevice as r WHERE r.MaterialId = m.ID AND r.DeviceId ='{0}'", device.ID));
            if (obj != null)
            {
                Guid materialID = (Guid)obj;
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("INSERT INTO dbo.MaterialEvents ");
                sb.AppendFormat("(ID,Type,Location1,Location2,CreateTime,Material_ID,Tag_ID) VALUES ");
                sb.AppendFormat("('{0}','{1}','{2}','{3}','{4}','{5}','{6}')",
                    Guid.NewGuid(), type, location1, Guid.Empty,
                    DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"),
                    materialID.ToString(), device.ID);
                Console.WriteLine(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss") + materialID.ToString() + device.ID);
                MSSQL.Instance().MySqlExcute(sb.ToString());


                //{
                //    try
                //    {
                //        using (DataClasses1DataContext newdb = new DataClasses1DataContext())
                //        {   //DB取出Identifier
                //            string MaterialsIdentifier = (from c in newdb.Materials where c.ID == materialID select c.Identifier).FirstOrDefault();
                //            string deviceIdentifier = (from c in newdb.Devices where c.ID == device.ID select c.Identifier).FirstOrDefault();
                //            string location1Identifier = (from c in newdb.Locations where c.ID == location1 select c.Name).FirstOrDefault();


                //            List<string> newLines = new List<string>();
                //            string row = String.Format("{0},{1},{2},{3},{4}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"), MaterialsIdentifier, deviceIdentifier, type, location1Identifier);
                //            newLines.Add(row);
                //            File.AppendAllLines(@".\LogFiles\" + DateTime.Now.ToString("yyyyMMdd") + ".csv", newLines);//寫入csv
                //        }
                //    }
                //    catch (Exception ex)
                //    {
                //        Trace.WriteLine(ex.StackTrace);
                //    }
                //}


                //{
                try
                {
                    var objMaterialIdentifier = MSSQL.Instance().MySqlExcuteScalar(
            string.Format("SELECT TOP 1 Identifier FROM dbo.Materials as m, dbo.MaterialDevice as r WHERE r.MaterialId = m.ID AND r.DeviceId ='{0}'", device.ID));

                    var objDeviceIdentifier = MSSQL.Instance().MySqlExcuteScalar(
                    string.Format("SELECT TOP 1 Identifier FROM dbo.Devices WHERE ID ='{0}'", device.ID));

                    var objLocation1Identifier = MSSQL.Instance().MySqlExcuteScalar(
                    string.Format("SELECT TOP 1 Name FROM dbo.Locations WHERE ID ='{0}'", location1));


                    string row = String.Format("{0},{1},{2},{3},{4}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"),
                        "" + (string)objMaterialIdentifier, "" + (string)objDeviceIdentifier, type, "" + (string)objLocation1Identifier);


                    Trace.WriteLine("/MOVELOG - " + row);

                    List<string> newLines = new List<string>();

                    newLines.Add(row);


                    File.AppendAllLines(@".\LogFiles\" + DateTime.Now.ToString("yyyyMMdd") + ".csv", newLines);//寫入csv
                }
                catch (Exception ex)
                {
                    Trace.WriteLine(ex.StackTrace);
                }




            }
        }

        private void LocationEnterEvent(string type, Guid location1, Guid location2, Device device)
        {
            var obj = MSSQL.Instance().MySqlExcuteScalar(
                string.Format("SELECT TOP 1 ID FROM dbo.Materials as m, dbo.MaterialDevice as r WHERE r.MaterialId = m.ID AND r.DeviceId ='{0}'AND m.enabled ='True'", device.ID));
            //string.Format("SELECT TOP 1 ID FROM dbo.Materials as m, dbo.MaterialDevice as r WHERE r.MaterialId = m.ID AND r.DeviceId ='{0}'", device.ID));
            if (obj != null)
            {
                Guid materialID = (Guid)obj;
                StringBuilder sb = new StringBuilder();
                sb.AppendFormat("INSERT INTO dbo.MaterialEvents ");
                sb.AppendFormat("(ID,Type,Location1,Location2,CreateTime,Material_ID,Tag_ID) VALUES ");
                sb.AppendFormat("('{0}','{1}','{2}','{3}','{4}','{5}','{6}')",
                    Guid.NewGuid(), type, location1,
                    location2, DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"),
                    materialID.ToString(), device.ID);
                MSSQL.Instance().MySqlExcute(sb.ToString());

                //{
                //    try
                //    {
                //        using (DataClasses1DataContext newdb = new DataClasses1DataContext())
                //        {
                //            //DB取出Identifier
                //            string MaterialsIdentifier = (from c in newdb.Materials where c.ID == materialID select c.Identifier).FirstOrDefault();
                //            string deviceIdentifier = (from c in newdb.Devices where c.ID == device.ID select c.Identifier).FirstOrDefault();
                //            string location1Identifier = (from c in newdb.Locations where c.ID == location1 select c.Name).FirstOrDefault();
                //            string location2Identifier = (from c in newdb.Locations where c.ID == location2 select c.Name).FirstOrDefault();

                //            List<string> newLines = new List<string>();
                //            string row = String.Format("{0},{1},{2},{3},{4},{5}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"), MaterialsIdentifier, deviceIdentifier, type, location1Identifier, location2Identifier);
                //            newLines.Add(row);
                //            File.AppendAllLines(@".\LogFiles\" + DateTime.Now.ToString("yyyyMMdd") + ".csv", newLines);//寫入csv
                //        }
                //    }
                //    catch (Exception ex)
                //    {
                //        Trace.WriteLine(ex.StackTrace);
                //    }
                //}


                //{
                try
                {
                    var objMaterialIdentifier = MSSQL.Instance().MySqlExcuteScalar(
            string.Format("SELECT TOP 1 Identifier FROM dbo.Materials as m, dbo.MaterialDevice as r WHERE r.MaterialId = m.ID AND r.DeviceId ='{0}'", device.ID));

                    var objDeviceIdentifier = MSSQL.Instance().MySqlExcuteScalar(
                    string.Format("SELECT TOP 1 Identifier FROM dbo.Devices WHERE ID ='{0}'", device.ID));

                    var objLocation1Identifier = MSSQL.Instance().MySqlExcuteScalar(
                    string.Format("SELECT TOP 1 Name FROM dbo.Locations WHERE ID ='{0}'", location1));

                    var objLocation2Identifier = MSSQL.Instance().MySqlExcuteScalar(
                    string.Format("SELECT TOP 1 Name FROM dbo.Locations WHERE ID ='{0}'", location2));

                    string row = String.Format("{0},{1},{2},{3},{4},{5}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"),
                        "" + (string)objMaterialIdentifier, "" + (string)objDeviceIdentifier, type,
                        "" + (string)objLocation1Identifier, "" + (string)objLocation2Identifier);


                    Trace.WriteLine("/MOVELOG - " + row);

                    List<string> newLines = new List<string>();

                    newLines.Add(row);


                    File.AppendAllLines(@".\LogFiles\" + DateTime.Now.ToString("yyyyMMdd") + ".csv", newLines);//寫入csv
                }
                catch (Exception ex)
                {
                    Trace.WriteLine(ex.StackTrace);
                }
            }
        }

        private void BridgeUpdate(Bridge bridge)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("UPDATE Bridges SET ");
            sb.AppendFormat("LastReport='{0}',Online={1} ",
                DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"),
                bridge.Online ? 1 : 0);
            sb.AppendFormat("WHERE UUID='{0}'", bridge.UUID);

            MSSQL.Instance().MySqlExcute(sb.ToString());

            viewFromRefresh();

            Trace.WriteLine(string.Format("BridgeUpdate {0} online {1} {2}", bridge.UUID, bridge.Online, DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")));
        }

        private void DeviceUpdate(Device device)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("UPDATE Devices SET ", MSSQL.Instance().Database);
            sb.AppendFormat("Battery={0},BridgeID='{1}',LastReport='{2}',Online={3},RSSI={4},BridgeChannel={5} ",
                device.Battery, device.Last_Bridge, DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"),
                device.Online ? 1 : 0, device.RSSI, device.BridgeChannel);
            sb.AppendFormat("WHERE UUID='{0}'", device.UUID);

            MSSQL.Instance().MySqlExcute(sb.ToString());

            viewFromRefresh();

            //Trace.WriteLine("DeviceUpdate");
        }

        private void MessageUpdate(int messageId, int status)
        {
            string sql;/*
            if (status == 1)
            {
                sql = "INSERT INTO[messageEnds] (UUID,Identifier, username,typeOf,message,urgent,datetime,groupName, readed)(SELECT UUID,Identifier, username,typeOf,message,urgent,datetime,groupName, 1 FROM[messageStarts])";
            }
            else
            {
                sql = string.Format("UPDATE messageEnds SET readed={1} WHERE id={2}", MSSQL.Instance().Database, status, messageId);
            }
            */
            /*
          sql = string.Format("UPDATE messageStarts SET readed={1},datetimeEnd='{3}' WHERE id={2}", MSSQL.Instance().Database, status, messageId, DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"));
          MSSQL.Instance().MySqlExcute(sql);*/
            //Trace.WriteLine("MessageUpdate");

            var selectmessageStart1 = from c in newdb.messageStarts
                                 select c;

            var message = (from c in selectmessageStart1 where c.ID == messageId select c).FirstOrDefault();
            message.readed = status;
            message.datetimeEnd = DateTime.Parse(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"));
            newdb.SubmitChanges();
        }

        private int GetMessageID()
        {
            string sql = string.Format("select top 1 *  FROM messageStarts order by ID desc ");
            var id = MSSQL.Instance().MySqlExcuteScalar(sql);
            //int id = Convert.ToInt32(num);
            return Convert.ToInt32(id);
        }
        int count = 0;
        static DataClasses1DataContext newdb = new DataClasses1DataContext();
        IQueryable<messageStarts> selectmessageStart = from c in newdb.messageStarts
                                                       select c;
        private void timerMessage_Tick(object sender, EventArgs e)
        {
            /*
            List<string> newLines1 = new List<string>();
            string row1 = String.Format("timerstart {0}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"));
            newLines1.Add(row1);
            File.AppendAllLines(@".\LogFiles\" + "time" + ".txt", newLines1);//每個device的RSSI
            */
            timerMessage.Stop();
            /*
            while (count <= 3)
            {
                Thread.Sleep(1000);//延遲1000ms，也就是1秒
                count++;
            }*/
            //using (DataClasses1DataContext newdb = new DataClasses1DataContext())
            //{
            selectmessageStart = from c in newdb.messageStarts
                                 where c.readed == 0 || c.readed == 1
                                 select c;
            foreach (var i in selectmessageStart)
            {
                if (i.readed == 0 && i.sendTimes == 0)
                {
                    int error = -999;
                    try
                    {

                        if (i.message == "")
                        {

                            error = masterService.CmdDeviceBuzzer(i.UUID, i.ID, 12);
                            if (error == 0)
                            {
                                i.sendTimes += 1;
                                List<string> newLines = new List<string>();
                                string row = String.Format("{0}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"));
                                newLines.Add(row);
                                File.AppendAllLines(@".\LogFiles\" + "time" + ".txt", newLines);//每個device的RSSI
                            }
                        }

                        else
                        {
                            error = masterService.CmdDeviceMessage(i.UUID, i.ID, i.message, 12);
                            if (error == 0)
                                i.sendTimes += 1;
                        }
                        #region
                        newdb.SubmitChanges();
                        //error對應的意義
                        //ERROR_OK = 0;
                        //DEVICE_UUID = 1;
                        //BRIDGE_UUID = 2;
                        //NOAVAILABLE = 3;
                        //MESSAGE_ID = 4;
                        //DEVICE_TYPE = 5;
                        //BUZZER_ID = 6;
                        //MESSAGE_QUEUE_SIZE = 7;

                        #endregion
                        //發送message後將處理過的message搬到messageEnd
                        //var newrow = new messageEnds
                        //{
                        //    UUID = i.UUID,
                        //    message = i.message,
                        //    userName = i.userName,
                        //    urgent = i.urgent,
                        //    groupName = i.groupName,
                        //    datetime = i.datetime,
                        //    readed = 0
                        //};

                        //newdb.messageEnds.InsertOnSubmit(newrow);//insert db
                        //newdb.SubmitChanges();//執行
                    }
                    catch (Exception ex)
                    {
                        Trace.WriteLine(ex.Message + " error = " + error.ToString());
                    }
                }
                else if (i.groupName != null)
                {
                    //  messageToGroup(i);
                }
            }

            //}
            timerMessage.Start();
        }
        /*
        /// <summary>
        /// send message to group
        /// select table groupName and userData for device UUID
        /// </summary>
        /// <param name="groupName">use split to makesure How many groups?</param>
        /// <param name="messageStartView"></param>
        private void messageToGroup(messageStartView messageStartView)
        {
            string[] stm;

            DataClasses1DataContext newdb = new DataClasses1DataContext();
            stm = messageStartView.groupName.Split(',');

            for (int i = 0; i < stm.Length; i++)
            {

                var Group_userName = from c in newdb.groupName
                                     where c.groupName1 == stm[i]
                                     select c.userName;

                foreach (var d in Group_userName)
                {
                    var user = (from b in newdb.userData
                                where b.userName == d
                                select b).Single();

                    int a = masterService.CmdDeviceMessage(user.UUID, count, messageStartView.message, 12);

                    //發送message後將處理過的message搬到messageEnd
                    var newrow = new messageEnd
                    {
                        UUID = messageStartView.UUID,
                        message = messageStartView.message,
                        userName = messageStartView.userName,
                        urgent = messageStartView.urgent,
                        groupName = messageStartView.groupName,
                        datetime = messageStartView.datetime,
                        readed = 0
                    };

                    newdb.messageEnd.InsertOnSubmit(newrow);//insert db
                    newdb.SubmitChanges();//執行

                }
                newdb.Dispose();
            }

        }*/
        private BridgeConfigModel CreateBridgeConfig(Bridge bridge)
        {
            BridgeConfigModel config = new BridgeConfigModel();
            config.LocationCalPeriod = bridge.LocationCalPeriod;
            config.SelfReportPeriod = bridge.SelfReportPeriod;
            config.ScanPeriod = bridge.ScanPeriod;
            config.ReportPeriod = bridge.ReportPeriod;
            config.ScanSignalLimit = bridge.ScanSignalLimit;
            config.MessageSignalLimit = bridge.MessageSignalLimit;
            config.EnableTagFilter = bridge.EnableTagFilter;
            config.EnableBandFilter = bridge.EnableBandFilter;
            config.RSSI_Compensate = bridge.RSSI_Compensate;
            config.ProximityThres = bridge.ProximityThres;
            return config;
        }

        private void DataBase_CheckBridges()
        {
            Dictionary<string, Bridge> bridges = new Dictionary<string, Bridge>();

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SELECT * FROM dbo.Bridges");
            DataTable dt = MSSQL.Instance().GetMyDataTable(sb.ToString());

            foreach (DataRow row in dt.Rows)
            {
                Bridge bridge = new Bridge(row);
                bridges.Add(bridge.UUID, bridge);
            }

            List<string> removeList = new List<string>();
            List<string> updateList = new List<string>();
            DataController db = masterService.DataBase;

            // update or delete
            foreach (KeyValuePair<string, Bridge> item in _bridges)
            {
                if (bridges.ContainsKey(item.Key))
                {
                    Bridge newBridge = bridges[item.Key];

                    // update
                    if (!newBridge.UpdateAt.Equals(item.Value.UpdateAt))
                    {
                        updateList.Add(item.Key);
                    }
                }
                else
                {
                    // delete
                    removeList.Add(item.Key);
                }
            }

            // update
            foreach (string uuid in updateList)
            {
                Bridge bridge = _bridges[uuid];
                Bridge newBridge = bridges[uuid];
                if (bridge.Online)
                    masterService.CmdBridgeReboot(uuid);
                BridgeConfigModel config = CreateBridgeConfig(newBridge);
                db.UpdateBridge(uuid, config);
                _bridges[uuid] = newBridge;
            }

            // delete
            foreach (string uuid in removeList)
            {
                Bridge bridge = _bridges[uuid];

                if (bridge.Online)
                    masterService.CmdBridgeReboot(uuid);
                db.DeleteBridge(uuid);
                _bridges.Remove(uuid);
            }


            foreach (KeyValuePair<string, Bridge> item in bridges)
            {
                if (!_bridges.ContainsKey(item.Key))
                {
                    // new
                    BridgeConfigModel config = CreateBridgeConfig(item.Value);
                    db.AddBridge(UserID, item.Key, config);
                    _bridges.Add(item.Key, item.Value);
                }
            }
        }

        private void DataBase_CheckDevices()
        {
            Dictionary<string, Device> devices = new Dictionary<string, Device>();

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("SELECT * FROM dbo.Devices");
            DataTable dt = MSSQL.Instance().GetMyDataTable(sb.ToString());

            foreach (DataRow row in dt.Rows)
            {
                Device device = new Device(row);
                devices.Add(device.UUID, device);
            }


            DataController db = masterService.DataBase;

            // delete
            List<string> removeList = new List<string>();

            foreach (KeyValuePair<string, Device> item in _devices)
            {
                if (!devices.ContainsKey(item.Key))
                {
                    removeList.Add(item.Key);
                }
            }

            foreach (string uuid in removeList)
            {
                db.DeleteDevice(uuid);
                _devices.Remove(uuid);
            }

            // add new 
            foreach (KeyValuePair<string, Device> item in devices)
            {
                if (!_devices.ContainsKey(item.Key))
                {
                    // new
                    db.AddDevice(UserID, item.Key, GetDeviceType(item.Value));
                    _devices.Add(item.Key, item.Value);
                }
            }
        }

        private void DataBase_CheckDirty()
        {
            bool dirty = (bool)MSSQL.Instance().MySqlExcuteScalar2("SELECT Top 1 DataDirty FROM dbo.Settings");

            if (dirty)
            {
                DataBase_CheckBridges();
                DataBase_CheckDevices();

                string sql = string.Format("UPDATE dbo.Settings SET DataDirty=0");
                MSSQL.Instance().MySqlExcute(sql);

                masterService.DataBaseRefresh();
            }
        }

        private void UpdateServerInfo()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("UPDATE dbo.ServerInfoes SET LastReport='{0}'", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"));
            MSSQL.Instance().MySqlExcute(sb.ToString());
        }

        private void timerPeriod_Tick(object sender, EventArgs e)
        {


            timerPeriod.Stop();


            int delay = masterService.DelayStart();
            if (delay > 0)
            {
                lblStart.Text = delay.ToString("D2");
                lblStart.Visible = true;
            }
            else
            {
                lblStart.Visible = false;
            }

            periodCounter++;
            if (periodCounter >= 3)
            {
                UpdateServerInfo();
                periodCounter = 0;
            }

            DataBase_CheckDirty();

            //StringBuilder sb = new StringBuilder();
            //sb.AppendFormat("SELECT * FROM {0}.messages WHERE status=-1", MSSQL.Instance().Database);

            //DataTable dt = MSSQL.Instance().GetMyDataTable(sb.ToString());

            //foreach (DataRow row in dt.Rows)
            //{
            //    int messageId = int.Parse(row["id"].ToString());
            //    string text = row["data"].ToString();
            //    string deviceUUID = row["deviceUuid"].ToString();
            //    masterService.CmdDeviceMessage(deviceUUID, messageId, text);

            //    string sql = string.Format("UPDATE {0}.messages SET status=0 WHERE id={1}", MSSQL.Instance().Database, messageId);
            //    MSSQL.Instance().MySqlExcute(sql);
            //}



            timerPeriod.Start();
        }

        private void viewFromRefresh()
        {
            if (view_from != null)
            {
                this.Invoke((MethodInvoker)delegate
                {
                    view_from.RefreshData();
                });
            }
        }

        private void StopMaster()
        {
            exitMasterThread = true;

            //masterService.Terminate();



        }

        private void btnView_Click(object sender, EventArgs e)
        {
            view_from = new ViewDataBaseForm();
            view_from.FormClosed += Form_FormClosed;
            view_from.Show();

            //form.Location = new System.Drawing.Point(this.Location.X +this.Width - 50, this.Location.Y);
        }

        private void Form_FormClosed(object sender, FormClosedEventArgs e)
        {
            view_from = null;
        }

        private void btnSetting_Click(object sender, EventArgs e)
        {
            //SettingForm form = new SettingForm();

            //if (form.ShowDialog() == DialogResult.OK)
            //{
            //    IniFile ini = GetIniFile();

            //    // load setting
            //    _locationProcessPeriod = int.Parse(ini.IniReadValue("Setting", "LocationProcessPeriod"));
            //    _selfReportPeriod = int.Parse(ini.IniReadValue("Setting", "SelfReportPeriod"));

            //    _bridgeLostThreshold = int.Parse(ini.IniReadValue("Setting", "BridgeLostThreshold"));
            //    _deviceLostThreshold = int.Parse(ini.IniReadValue("Setting", "DeviceLostThreshold"));
            //}
        }
        /// <summary>
        ///  Close need password
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MainForm_FormClosing(object sender, FormClosingEventArgs e)
        {
            //ConnectionPool.Instance().Destroy();

            if (serviceRunning)
            {
                MessageBox.Show("Master Serive is running, Stop Service First.", "Warning", MessageBoxButtons.OK);
                e.Cancel = true;
                return;

                //StopBroker();
                //StopMaster();

                //Thread.Sleep(3000);
            }

            MSSQL.Instance().Destroy();

        }

        /// <summary>
        /// Stop need password
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnStop_Click(object sender, EventArgs e)
        {
            CloseForm form = new CloseForm();

            if (form.ShowDialog() == DialogResult.OK)
            {
                StopMaster();
                StopBroker();
                dataClear();
                Thread.Sleep(3000);
            }

        }

        private void btnLogClear_Click(object sender, EventArgs e)
        {
            lstLog.Items.Clear();
        }

        private void btnExit_Click(object sender, EventArgs e)
        {
            if (serviceRunning)
            {
                MessageBox.Show("Master Serive is running, Stop Service First.", "Warning", MessageBoxButtons.OK);
                return;

                //StopBroker();
                //StopMaster();

                //Thread.Sleep(3000);
            }

            this.Close();
        }

        private void btnBurn_Click(object sender, EventArgs e)
        {
            BurnTestForm form = new BurnTestForm();

            form.ShowDialog();
        }

        private void btnlog_Click(object sender, EventArgs e)
        {
            if (isRawDataChangedLog)
            {
                isRawDataChangedLog = false;
                btnlog.Text = "RAW-LOG\r\nSTART";
            }
            else
            {
                isRawDataChangedLog = true;
                btnlog.Text = "RAW-LOG\r\nSTOP";
            }
        }

        private void timerTimeOutMessage_Tick(object sender, EventArgs e)
        {
            var selectTimeOutMessage = from c in newdb.messageStarts where c.readed == 3 select c;


            foreach (var i in selectTimeOutMessage)
            {/*
                i.sendTimes += 1;
                newdb.SubmitChanges();
                //var newmessage = i;
                var newmessage = new messageStarts
                {
                    UUID = i.UUID,
                    ID = i.ID + 500,
                    readed = 0,
                    sendTimes = 0,
                    message = i.message,
                    datetime = DateTime.Parse(DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"))
                };

                newdb.messageStarts.InsertOnSubmit(newmessage);
                newdb.SubmitChanges();*/

                    int error = -999;
                    try
                    {

                        if (i.message == "")
                        {

                            error = masterService.CmdDeviceBuzzer(i.UUID, i.ID, 12);
                            if (error == 0)
                            {
                                i.sendTimes += 1;
                                List<string> newLines = new List<string>();
                                string row = String.Format("{0}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"));
                                newLines.Add(row);
                                File.AppendAllLines(@".\LogFiles\" + "time" + ".txt", newLines);//每個device的RSSI
                            }
                        }

                        else
                        {
                            error = masterService.CmdDeviceMessage(i.UUID, i.ID, i.message, 12);
                            if (error == 0)
                            {
                                i.sendTimes += 1;
                                List<string> newLines = new List<string>();
                                string row = String.Format("{0}{1}{2}{3}", DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss"),i.UUID, i.ID);
                                newLines.Add(row);
                                File.AppendAllLines(@".\LogFiles\" + "time" + ".txt", newLines);
                            }//每個device的RSSI
                        }
                        #region 
                        newdb.SubmitChanges();
                        //error對應的意義
                        //ERROR_OK = 0;
                        //DEVICE_UUID = 1;
                        //BRIDGE_UUID = 2;
                        //NOAVAILABLE = 3;
                        //MESSAGE_ID = 4;
                        //DEVICE_TYPE = 5;
                        //BUZZER_ID = 6;
                        //MESSAGE_QUEUE_SIZE = 7;

                        #endregion
                        //發送message後將處理過的message搬到messageEnd
                        //var newrow = new messageEnds
                        //{
                        //    UUID = i.UUID,
                        //    message = i.message,
                        //    userName = i.userName,
                        //    urgent = i.urgent,
                        //    groupName = i.groupName,
                        //    datetime = i.datetime,
                        //    readed = 0
                        //};

                        //newdb.messageEnds.InsertOnSubmit(newrow);//insert db
                        //newdb.SubmitChanges();//執行
                    }
                    catch (Exception ex)
                    {
                        Trace.WriteLine(ex.Message + " error = " + error.ToString());
                    }
                
            }
        }
    }
}
