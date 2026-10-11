package model

import "time"

// HomeSuggestion 新对话首页输入框下方的一张建议卡（每卡一行）。
//
// 由 Core coredata 迁移并 seed-if-absent；管理端整表覆盖写入。
// Revision 是乐观锁：每次整表写入把所有行统一置为 旧 max(revision)+1，
// 客户端回传读到的 max(revision)，不一致即 409。
//
// 回滚：表是纯新增，旧 Core 不读它；回滚 Core 后表残留无害，
// 需要清理时 DROP TABLE home_suggestions。
type HomeSuggestion struct {
	ID           string    `json:"id" gorm:"primaryKey;size:40;comment:卡片ID"`
	Enabled      bool      `json:"enabled" gorm:"not null;comment:是否启用"`
	SortOrder    int       `json:"sortOrder" gorm:"not null;default:0;comment:排序(升序)"`
	Icon         string    `json:"icon" gorm:"size:40;not null;comment:内置图标ID"`
	Action       string    `json:"action" gorm:"size:20;not null;comment:点击行为 fill-title|prefill"`
	TitleZh      string    `json:"titleZh" gorm:"size:80;not null;comment:中文标题"`
	DescZh       string    `json:"descZh" gorm:"size:160;not null;default:'';comment:中文描述"`
	PromptZh     string    `json:"promptZh" gorm:"type:text;comment:中文预填内容"`
	TitleEn      string    `json:"titleEn" gorm:"size:80;not null;default:'';comment:英文标题(空则用中文)"`
	DescEn       string    `json:"descEn" gorm:"size:160;not null;default:'';comment:英文描述(空则用中文)"`
	PromptEn     string    `json:"promptEn" gorm:"type:text;comment:英文预填内容(空则用中文)"`
	TemplateName *string   `json:"templateName,omitempty" gorm:"size:128;comment:绑定模板(二期预留)"`
	Revision     uint      `json:"revision" gorm:"not null;default:1;comment:乐观锁版本"`
	CreatedAt    time.Time `json:"createdAt" gorm:"comment:创建时间"`
	UpdatedAt    time.Time `json:"updatedAt" gorm:"comment:更新时间"`
	UpdatedBy    uint      `json:"updatedBy" gorm:"not null;default:0;comment:最后修改人(0=种子)"`
}

func (HomeSuggestion) TableName() string { return "home_suggestions" }

// 点击行为。二期再加 "template"。
const (
	HomeSuggestionActionFillTitle = "fill-title"
	HomeSuggestionActionPrefill   = "prefill"
)

// HomeSuggestionMaxItems 卡片数量上限。
const HomeSuggestionMaxItems = 12

// HomeSuggestionIcons 内置图标 ID 白名单（前端 core-web 有同一份 ID→SVG 映射）。
// 前 9 个是上线前写死的图标，ID 永久保留。
var HomeSuggestionIcons = []string{
	// 原 9 个
	"check", "ppt", "chat", "video", "music", "chart", "doc", "clock", "layers",
	// 通用
	"star", "sparkles", "lightbulb", "zap", "search", "bot", "rocket",
	// 办公
	"mail", "calendar", "briefcase", "clipboard-list", "folder", "list-todo", "users",
	// 数据分析
	"chart-column", "chart-pie", "trending-up", "database", "table", "calculator", "gauge", "sheet",
	// 写作
	"pen-line", "notebook-pen", "book-open", "quote", "languages", "newspaper", "scroll-text",
	// 代码开发
	"code", "terminal", "bug", "git-branch", "braces", "server", "cpu", "workflow", "puzzle",
	// 设计
	"palette", "pen-tool", "brush", "shapes", "layout-dashboard",
	// 多媒体
	"mic", "camera", "image",
	// 营销
	"megaphone", "target", "shopping-cart", "tag", "handshake", "share-2", "badge-percent", "store",
	// 教育
	"graduation-cap", "school", "book", "brain", "flask-conical", "globe", "library", "award",
	// 生活
	"heart", "coffee", "plane", "map-pin", "utensils", "dumbbell", "house", "sun", "gift",
}

// DefaultHomeSuggestions 种子 / “恢复默认”：现行产品能力的 8 张卡（UNI-C7），点击填入预填全文，不自动发送。
func DefaultHomeSuggestions() []HomeSuggestion {
	row := func(order int, id, icon, titleZh, descZh, promptZh, titleEn, descEn, promptEn string) HomeSuggestion {
		return HomeSuggestion{ID: id, Enabled: true, SortOrder: order, Icon: icon, Action: HomeSuggestionActionPrefill,
			TitleZh: titleZh, DescZh: descZh, PromptZh: promptZh, TitleEn: titleEn, DescEn: descEn, PromptEn: promptEn, Revision: 1}
	}
	return []HomeSuggestion{
		row(10, "writeCode", "code", "写代码", "写脚本、修 Bug、读懂代码",
			"帮我写一段代码：\n- 语言 / 框架：\n- 要实现的功能：\n- 输入和输出示例：\n写完请附上运行方法和简单的测试。",
			"Write Code", "Scripts, bug fixes, code walkthroughs",
			"Write some code for me:\n- Language / framework:\n- What it should do:\n- Example input and output:\nInclude how to run it and a simple test."),
		row(20, "research", "search", "查资料", "联网搜索，整理要点并附来源",
			"帮我查一下：\n\n请联网搜索最新资料，分点整理要点，并附上来源链接。",
			"Research", "Search the web, key points with sources",
			"Look this up for me:\n\nSearch the web for current sources, summarize the key points and link the sources."),
		row(30, "analyzeData", "chart-column", "做分析", "上传表格，得出结论和图表",
			"请分析我上传的数据（点输入框左下角的回形针上传 Excel / CSV）：\n1. 概览字段和数据质量\n2. 找出关键趋势和异常\n3. 画出图表并给出结论",
			"Analyze Data", "Upload a sheet, get findings and charts",
			"Analyze the data I upload (use the paperclip below the input to attach Excel / CSV):\n1. Overview of fields and data quality\n2. Key trends and anomalies\n3. Charts and conclusions"),
		row(40, "writeReport", "doc", "写报告", "生成文档，保存到成果库",
			"帮我写一份报告，并把最终文档保存为文件（会出现在成果库）：\n- 主题：\n- 读者：\n- 篇幅：约 字",
			"Write a Report", "Draft a document, saved to Deliverables",
			"Write a report and save the final document as a file (it shows up in Deliverables):\n- Topic:\n- Audience:\n- Length: about words"),
		row(50, "teamMessage", "users", "群协作", "写群通知、会议纪要，发到 IM 群",
			"把下面的内容整理成一条可以直接发到 IM 群的消息：简洁分点，列出待办、负责人和截止时间。\n\n",
			"Team Update", "Group notices and minutes for IM",
			"Turn the notes below into a message I can post in an IM group: short bullet points with to-dos, owners and due dates.\n\n"),
		row(60, "generateImage", "image", "生成图片", "描述画面，AI 帮你出图",
			"帮我生成一张图片：\n- 画面内容：\n- 风格（写实 / 插画 / 扁平等）：\n- 比例（1:1、16:9 等）：",
			"Generate Image", "Describe a scene, get an image",
			"Generate an image:\n- What it shows:\n- Style (photo, illustration, flat...):\n- Aspect ratio (1:1, 16:9...):"),
		row(70, "generateVideo", "video", "生成视频", "文字描述生成短视频",
			"帮我生成一段短视频：\n- 画面和动作：\n- 时长（如 5 秒）：\n- 比例（16:9 / 9:16）：",
			"Generate Video", "Turn a description into a short clip",
			"Generate a short video:\n- Scene and motion:\n- Length (e.g. 5 s):\n- Aspect ratio (16:9 / 9:16):"),
		row(80, "translate", "languages", "翻译润色", "中英互译，改得更通顺地道",
			"请把下面的内容翻译成英文（如果原文是英文就译成中文），并润色得通顺地道，保留原有格式：\n\n",
			"Translate & Polish", "Chinese-English, natural and fluent",
			"Translate the text below into Chinese (or into English if it is Chinese) and polish it so it reads naturally. Keep the formatting:\n\n"),
	}
}

// RetiredHomeSuggestionDefaults 以前版本的系统预置卡（T14 上线的 9 张旧卡，只填标题、无预填内容）。
// 只用作“系统预置且未被管理员改过”的内容指纹：表里恰好是其中某一套时，Core 启动会换成现行默认；
// 管理员改过任何一个字段、增删过卡，都不再匹配，保持不动。以后再换默认时，把被换下的那套追加到这里。
func RetiredHomeSuggestionDefaults() [][]HomeSuggestion {
	row := func(order int, id, icon, titleZh, descZh, titleEn, descEn string) HomeSuggestion {
		return HomeSuggestion{ID: id, Enabled: true, SortOrder: order, Icon: icon, Action: HomeSuggestionActionFillTitle,
			TitleZh: titleZh, DescZh: descZh, TitleEn: titleEn, DescEn: descEn, Revision: 1}
	}
	return [][]HomeSuggestion{{
		row(10, "competitiveAnalysis", "check", "竞品分析", "自动收集数据生成报告", "Competitive Analysis", "Auto-collect data and generate reports"),
		row(20, "presentation", "ppt", "演示文稿", "从大纲到 PPT 一键生成", "Presentation", "Outline to PPT in one click"),
		row(30, "freeChat", "chat", "自由对话", "随时 brainstorm 或问答", "Free Chat", "Brainstorm or Q&A anytime"),
		row(40, "videoGen", "video", "视频生成", "文字描述生成短视频", "Video Generation", "Generate short videos from text"),
		row(50, "musicCreation", "music", "音乐创作", "AI 辅助编曲与生成", "Music Creation", "AI-assisted composing and generation"),
		row(60, "dataAnalysis", "chart", "数据分析", "上传数据自动可视化", "Data Analysis", "Upload data for auto-visualization"),
		row(70, "docWriting", "doc", "文档撰写", "自动生成长文和报告", "Document Writing", "Auto-generate long-form content and reports"),
		row(80, "workflowAutomation", "clock", "流程自动化", "连接 API 自动执行", "Workflow Automation", "Connect APIs for automated execution"),
		row(90, "knowledgeQA", "layers", "知识库问答", "基于文档智能问答", "Knowledge Base Q&A", "Smart Q&A based on documents"),
	}}
}

// IsRetiredHomeSuggestionDefault 表内容（忽略 revision、时间戳、修改人）是否恰好等于某一套旧的系统预置。
func IsRetiredHomeSuggestionDefault(rows []HomeSuggestion) bool {
	type card struct {
		Enabled                                                                      bool
		SortOrder                                                                    int
		Icon, Action, TitleZh, DescZh, PromptZh, TitleEn, DescEn, PromptEn, Template string
	}
	content := func(list []HomeSuggestion) map[string]card {
		m := make(map[string]card, len(list))
		for _, r := range list {
			tpl := ""
			if r.TemplateName != nil {
				tpl = *r.TemplateName
			}
			m[r.ID] = card{r.Enabled, r.SortOrder, r.Icon, r.Action, r.TitleZh, r.DescZh, r.PromptZh, r.TitleEn, r.DescEn, r.PromptEn, tpl}
		}
		return m
	}
	have := content(rows)
	for _, set := range RetiredHomeSuggestionDefaults() {
		want := content(set)
		if len(want) != len(rows) || len(have) != len(rows) {
			continue
		}
		same := true
		for id, c := range want {
			if have[id] != c {
				same = false
				break
			}
		}
		if same {
			return true
		}
	}
	return false
}
