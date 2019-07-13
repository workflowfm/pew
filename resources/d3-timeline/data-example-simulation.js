var tasks = [
	"AwardContract",
	"ProvideService",
	"TaskSimTask",
	"CheckOutcome",
];

var resourceData = [
{label: "Petros", times: [
	{"label":"AwardContract(D2)", task: "AwardContract", "starting_time": 0, "ending_time": 1, delay: 0, cost: 2},
	{"label":"TaskSimTask(TaskSim)", task: "TaskSimTask", "starting_time": 6, "ending_time": 11, delay: 0, cost: 15},
	{"label":"CheckOutcome(D2)", task: "CheckOutcome", "starting_time": 11, "ending_time": 13, delay: 5, cost: 3},
	{"label":"AwardContract(A1)", task: "AwardContract", "starting_time": 13, "ending_time": 14, delay: 4, cost: 2},
	{"label":"AwardContract(A3)", task: "AwardContract", "starting_time": 14, "ending_time": 15, delay: 3, cost: 2},
	{"label":"AwardContract(A7)", task: "AwardContract", "starting_time": 22, "ending_time": 23, delay: 0, cost: 2},
	{"label":"ProvideService(D4)", task: "ProvideService", "starting_time": 23, "ending_time": 28, delay: 1, cost: 6},
	{"label":"AwardContract(D8)", task: "AwardContract", "starting_time": 28, "ending_time": 29, delay: 6, cost: 2},
]},
{label: "Orphen", times: [
	{"label":"ProvideService(D2)", task: "ProvideService", "starting_time": 1, "ending_time": 6, delay: 0, cost: 6},
	{"label":"TaskSimTask(TaskSim)", task: "TaskSimTask", "starting_time": 6, "ending_time": 11, delay: 0, cost: 15},
	{"label":"ProvideService(A5)", task: "ProvideService", "starting_time": 11, "ending_time": 16, delay: 1, cost: 6},
	{"label":"ProvideService(D6)", task: "ProvideService", "starting_time": 16, "ending_time": 21, delay: 5, cost: 6},
	{"label":"AwardContract(D4)", task: "AwardContract", "starting_time": 21, "ending_time": 22, delay: 9, cost: 2},
	{"label":"ProvideService(A1)", task: "ProvideService", "starting_time": 22, "ending_time": 27, delay: 8, cost: 6},
	{"label":"ProvideService(A3)", task: "ProvideService", "starting_time": 27, "ending_time": 32, delay: 12, cost: 6},
	{"label":"CheckOutcome(A5)", task: "CheckOutcome", "starting_time": 32, "ending_time": 34, delay: 16, cost: 3},
	{"label":"CheckOutcome(D4)", task: "CheckOutcome", "starting_time": 34, "ending_time": 36, delay: 6, cost: 3},
	{"label":"ProvideService(A7)", task: "ProvideService", "starting_time": 37, "ending_time": 42, delay: 14, cost: 6},
	{"label":"CheckOutcome(A1)", task: "CheckOutcome", "starting_time": 42, "ending_time": 44, delay: 15, cost: 3},
	{"label":"CheckOutcome(A3)", task: "CheckOutcome", "starting_time": 44, "ending_time": 46, delay: 12, cost: 3},
	{"label":"CheckOutcome(A7)", task: "CheckOutcome", "starting_time": 46, "ending_time": 48, delay: 4, cost: 3},
]},
{label: "Patient1", times: [
	{"label":"ProvideService(D2)", task: "ProvideService", "starting_time": 1, "ending_time": 6, delay: 0, cost: 6},
	{"label":"CheckOutcome(D2)", task: "CheckOutcome", "starting_time": 11, "ending_time": 13, delay: 5, cost: 3},
	{"label":"ProvideService(A1)", task: "ProvideService", "starting_time": 22, "ending_time": 27, delay: 8, cost: 6},
	{"label":"ProvideService(A3)", task: "ProvideService", "starting_time": 27, "ending_time": 32, delay: 12, cost: 6},
	{"label":"ProvideService(D8)", task: "ProvideService", "starting_time": 32, "ending_time": 37, delay: 3, cost: 6},
	{"label":"ProvideService(A7)", task: "ProvideService", "starting_time": 37, "ending_time": 42, delay: 14, cost: 6},
	{"label":"CheckOutcome(A1)", task: "CheckOutcome", "starting_time": 42, "ending_time": 44, delay: 15, cost: 3},
	{"label":"CheckOutcome(A3)", task: "CheckOutcome", "starting_time": 44, "ending_time": 46, delay: 12, cost: 3},
	{"label":"CheckOutcome(A7)", task: "CheckOutcome", "starting_time": 46, "ending_time": 48, delay: 4, cost: 3},
]},
{label: "Blah", times: [
	{"label":"AwardContract(A5)", task: "AwardContract", "starting_time": 9, "ending_time": 10, delay: 0, cost: 2},
	{"label":"AwardContract(D6)", task: "AwardContract", "starting_time": 10, "ending_time": 11, delay: 1, cost: 2},
	{"label":"ProvideService(D8)", task: "ProvideService", "starting_time": 32, "ending_time": 37, delay: 3, cost: 6},
]},
{label: "Patient3", times: [
	{"label":"ProvideService(A5)", task: "ProvideService", "starting_time": 11, "ending_time": 16, delay: 1, cost: 6},
	{"label":"ProvideService(D6)", task: "ProvideService", "starting_time": 16, "ending_time": 21, delay: 5, cost: 6},
	{"label":"CheckOutcome(A5)", task: "CheckOutcome", "starting_time": 32, "ending_time": 34, delay: 16, cost: 3},
]},
{label: "Patient2", times: [
	{"label":"ProvideService(D4)", task: "ProvideService", "starting_time": 23, "ending_time": 28, delay: 1, cost: 6},
	{"label":"CheckOutcome(D4)", task: "CheckOutcome", "starting_time": 34, "ending_time": 36, delay: 6, cost: 3},
]},
];

var simulationData = [
{label: "D2", times: [
	{"label":"AwardContract(D2)", task: "AwardContract", "starting_time": 0, "ending_time": 1, delay: 0, cost: 2},
	{"label":"ProvideService(D2)", task: "ProvideService", "starting_time": 1, "ending_time": 6, delay: 0, cost: 6},
	{"label":"CheckOutcome(D2)", task: "CheckOutcome", "starting_time": 11, "ending_time": 13, delay: 5, cost: 3},
]},
{label: "TaskSim", times: [
	{"label":"TaskSimTask(TaskSim)", task: "TaskSimTask", "starting_time": 6, "ending_time": 11, delay: 0, cost: 15},
]},
{label: "A1", times: [
	{"label":"AwardContract(A1)", task: "AwardContract", "starting_time": 13, "ending_time": 14, delay: 4, cost: 2},
	{"label":"ProvideService(A1)", task: "ProvideService", "starting_time": 22, "ending_time": 27, delay: 8, cost: 6},
	{"label":"CheckOutcome(A1)", task: "CheckOutcome", "starting_time": 42, "ending_time": 44, delay: 15, cost: 3},
]},
{label: "A5", times: [
	{"label":"AwardContract(A5)", task: "AwardContract", "starting_time": 9, "ending_time": 10, delay: 0, cost: 2},
	{"label":"ProvideService(A5)", task: "ProvideService", "starting_time": 11, "ending_time": 16, delay: 1, cost: 6},
	{"label":"CheckOutcome(A5)", task: "CheckOutcome", "starting_time": 32, "ending_time": 34, delay: 16, cost: 3},
]},
{label: "D6", times: [
	{"label":"AwardContract(D6)", task: "AwardContract", "starting_time": 10, "ending_time": 11, delay: 1, cost: 2},
	{"label":"ProvideService(D6)", task: "ProvideService", "starting_time": 16, "ending_time": 21, delay: 5, cost: 6},
]},
{label: "A3", times: [
	{"label":"AwardContract(A3)", task: "AwardContract", "starting_time": 14, "ending_time": 15, delay: 3, cost: 2},
	{"label":"ProvideService(A3)", task: "ProvideService", "starting_time": 27, "ending_time": 32, delay: 12, cost: 6},
	{"label":"CheckOutcome(A3)", task: "CheckOutcome", "starting_time": 44, "ending_time": 46, delay: 12, cost: 3},
]},
{label: "D4", times: [
	{"label":"AwardContract(D4)", task: "AwardContract", "starting_time": 21, "ending_time": 22, delay: 9, cost: 2},
	{"label":"ProvideService(D4)", task: "ProvideService", "starting_time": 23, "ending_time": 28, delay: 1, cost: 6},
	{"label":"CheckOutcome(D4)", task: "CheckOutcome", "starting_time": 34, "ending_time": 36, delay: 6, cost: 3},
]},
{label: "A7", times: [
	{"label":"AwardContract(A7)", task: "AwardContract", "starting_time": 22, "ending_time": 23, delay: 0, cost: 2},
	{"label":"ProvideService(A7)", task: "ProvideService", "starting_time": 37, "ending_time": 42, delay: 14, cost: 6},
	{"label":"CheckOutcome(A7)", task: "CheckOutcome", "starting_time": 46, "ending_time": 48, delay: 4, cost: 3},
]},
{label: "D8", times: [
	{"label":"AwardContract(D8)", task: "AwardContract", "starting_time": 28, "ending_time": 29, delay: 6, cost: 2},
	{"label":"ProvideService(D8)", task: "ProvideService", "starting_time": 32, "ending_time": 37, delay: 3, cost: 6},
]},
];
