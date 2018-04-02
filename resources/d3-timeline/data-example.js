var totalTicks = 46
var widthPerTick = (1024/totalTicks)

var tasks = [
	"TaskSimTask",
	"AwardContract",
	"CheckOutcome",
	"ProvideService",
];

var resourceData = [
{label: "Petros", times: [
	{"label":"CheckOutcome(D2)", task: "CheckOutcome", "starting_time": 43200000, "ending_time": 50400000},
	{"label":"CheckOutcome(A5)", task: "CheckOutcome", "starting_time": 79200000, "ending_time": 86400000},
	{"label":"AwardContract(A3)", task: "AwardContract", "starting_time": 54000000, "ending_time": 57600000},
	{"label":"TaskSimTask(TaskSim)", task: "TaskSimTask", "starting_time": 25200000, "ending_time": 43200000},
	{"label":"AwardContract(D8)", task: "AwardContract", "starting_time": 90000000, "ending_time": 93600000},
	{"label":"AwardContract(D2)", task: "AwardContract", "starting_time": 3600000, "ending_time": 7200000},
	{"label":"AwardContract(A7)", task: "AwardContract", "starting_time": 86400000, "ending_time": 90000000},
	{"label":"ProvideService(A5)", task: "ProvideService", "starting_time": 61200000, "ending_time": 79200000},
	{"label":"AwardContract(A1)", task: "AwardContract", "starting_time": 50400000, "ending_time": 54000000},
	{"label":"CheckOutcome(D8)", task: "CheckOutcome", "starting_time": 126000000, "ending_time": 133200000},
]},
{label: "Patient1", times: [
	{"label":"CheckOutcome(D2)", task: "CheckOutcome", "starting_time": 43200000, "ending_time": 50400000},
	{"label":"CheckOutcome(A7)", task: "CheckOutcome", "starting_time": 158400000, "ending_time": 165600000},
	{"label":"ProvideService(A3)", task: "ProvideService", "starting_time": 82800000, "ending_time": 100800000},
	{"label":"CheckOutcome(A1)", task: "CheckOutcome", "starting_time": 100800000, "ending_time": 108000000},
	{"label":"ProvideService(D8)", task: "ProvideService", "starting_time": 108000000, "ending_time": 126000000},
	{"label":"ProvideService(D2)", task: "ProvideService", "starting_time": 7200000, "ending_time": 25200000},
	{"label":"CheckOutcome(A3)", task: "CheckOutcome", "starting_time": 151200000, "ending_time": 158400000},
	{"label":"ProvideService(A7)", task: "ProvideService", "starting_time": 133200000, "ending_time": 151200000},
	{"label":"ProvideService(A1)", task: "ProvideService", "starting_time": 64800000, "ending_time": 82800000},
	{"label":"CheckOutcome(D8)", task: "CheckOutcome", "starting_time": 126000000, "ending_time": 133200000},
]},
{label: "Orphen", times: [
	{"label":"TaskSimTask(TaskSim)", task: "TaskSimTask", "starting_time": 25200000, "ending_time": 43200000},
	{"label":"ProvideService(D6)", task: "ProvideService", "starting_time": 43200000, "ending_time": 61200000},
	{"label":"CheckOutcome(A7)", task: "CheckOutcome", "starting_time": 158400000, "ending_time": 165600000},
	{"label":"ProvideService(A3)", task: "ProvideService", "starting_time": 82800000, "ending_time": 100800000},
	{"label":"CheckOutcome(D4)", task: "CheckOutcome", "starting_time": 108000000, "ending_time": 115200000},
	{"label":"CheckOutcome(A1)", task: "CheckOutcome", "starting_time": 100800000, "ending_time": 108000000},
	{"label":"AwardContract(D4)", task: "AwardContract", "starting_time": 61200000, "ending_time": 64800000},
	{"label":"ProvideService(D2)", task: "ProvideService", "starting_time": 7200000, "ending_time": 25200000},
	{"label":"CheckOutcome(A3)", task: "CheckOutcome", "starting_time": 151200000, "ending_time": 158400000},
	{"label":"ProvideService(A7)", task: "ProvideService", "starting_time": 133200000, "ending_time": 151200000},
	{"label":"ProvideService(A1)", task: "ProvideService", "starting_time": 64800000, "ending_time": 82800000},
]},
{label: "Blah", times: [
	{"label":"AwardContract(A5)", task: "AwardContract", "starting_time": 36000000, "ending_time": 39600000},
	{"label":"ProvideService(D8)", task: "ProvideService", "starting_time": 108000000, "ending_time": 126000000},
	{"label":"AwardContract(D6)", task: "AwardContract", "starting_time": 39600000, "ending_time": 43200000},
	{"label":"ProvideService(D4)", task: "ProvideService", "starting_time": 64800000, "ending_time": 82800000},
]},
{label: "Patient3", times: [
	{"label":"CheckOutcome(A5)", task: "CheckOutcome", "starting_time": 79200000, "ending_time": 86400000},
	{"label":"ProvideService(D6)", task: "ProvideService", "starting_time": 43200000, "ending_time": 61200000},
	{"label":"ProvideService(A5)", task: "ProvideService", "starting_time": 61200000, "ending_time": 79200000},
]},
{label: "Patient2", times: [
	{"label":"CheckOutcome(D4)", task: "CheckOutcome", "starting_time": 108000000, "ending_time": 115200000},
	{"label":"ProvideService(D4)", task: "ProvideService", "starting_time": 64800000, "ending_time": 82800000},
]},
];

var simulationData = [
{label: "D2", times: [
	{"label":"CheckOutcome(D2)", task: "CheckOutcome", "starting_time": 43200000, "ending_time": 50400000},
	{"label":"AwardContract(D2)", task: "AwardContract", "starting_time": 3600000, "ending_time": 7200000},
	{"label":"ProvideService(D2)", task: "ProvideService", "starting_time": 7200000, "ending_time": 25200000},
]},
{label: "TaskSim", times: [
	{"label":"TaskSimTask(TaskSim)", task: "TaskSimTask", "starting_time": 25200000, "ending_time": 43200000},
]},
{label: "A5", times: [
	{"label":"CheckOutcome(A5)", task: "CheckOutcome", "starting_time": 79200000, "ending_time": 86400000},
	{"label":"AwardContract(A5)", task: "AwardContract", "starting_time": 36000000, "ending_time": 39600000},
	{"label":"ProvideService(A5)", task: "ProvideService", "starting_time": 61200000, "ending_time": 79200000},
]},
{label: "A1", times: [
	{"label":"CheckOutcome(A1)", task: "CheckOutcome", "starting_time": 100800000, "ending_time": 108000000},
	{"label":"AwardContract(A1)", task: "AwardContract", "starting_time": 50400000, "ending_time": 54000000},
	{"label":"ProvideService(A1)", task: "ProvideService", "starting_time": 64800000, "ending_time": 82800000},
]},
{label: "D6", times: [
	{"label":"ProvideService(D6)", task: "ProvideService", "starting_time": 43200000, "ending_time": 61200000},
	{"label":"AwardContract(D6)", task: "AwardContract", "starting_time": 39600000, "ending_time": 43200000},
]},
{label: "A3", times: [
	{"label":"AwardContract(A3)", task: "AwardContract", "starting_time": 54000000, "ending_time": 57600000},
	{"label":"ProvideService(A3)", task: "ProvideService", "starting_time": 82800000, "ending_time": 100800000},
	{"label":"CheckOutcome(A3)", task: "CheckOutcome", "starting_time": 151200000, "ending_time": 158400000},
]},
{label: "D4", times: [
	{"label":"CheckOutcome(D4)", task: "CheckOutcome", "starting_time": 108000000, "ending_time": 115200000},
	{"label":"AwardContract(D4)", task: "AwardContract", "starting_time": 61200000, "ending_time": 64800000},
	{"label":"ProvideService(D4)", task: "ProvideService", "starting_time": 64800000, "ending_time": 82800000},
]},
{label: "D8", times: [
	{"label":"AwardContract(D8)", task: "AwardContract", "starting_time": 90000000, "ending_time": 93600000},
	{"label":"ProvideService(D8)", task: "ProvideService", "starting_time": 108000000, "ending_time": 126000000},
	{"label":"CheckOutcome(D8)", task: "CheckOutcome", "starting_time": 126000000, "ending_time": 133200000},
]},
{label: "A7", times: [
	{"label":"CheckOutcome(A7)", task: "CheckOutcome", "starting_time": 158400000, "ending_time": 165600000},
	{"label":"AwardContract(A7)", task: "AwardContract", "starting_time": 86400000, "ending_time": 90000000},
	{"label":"ProvideService(A7)", task: "ProvideService", "starting_time": 133200000, "ending_time": 151200000},
]},
];