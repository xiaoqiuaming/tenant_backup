# GitHub Copilot Agent Skills

本项目包含多个 GitHub Copilot Agent Skills，用于增强 AI 辅助开发能力。

##  已安装的 Skills

### 1. youtube-downloader
**功能**：下载 YouTube 视频，支持多种质量和格式选项

- 支持质量：best, 1080p, 720p, 480p, 360p, worst
- 支持格式：mp4, webm, mkv
- 支持音频提取（MP3）
- 自动安装依赖（yt-dlp）

**使用示例**：
```
"下载这个 YouTube 视频：https://www.youtube.com/watch?v=xxx"
"下载这个视频的 720p 版本"
"把这个视频的音频提取成 MP3"
```

 [详细文档](./video-downloader/README.md)

---

### 2. youtube-transcript
**功能**：下载 YouTube 视频的字幕/转录文本

- 优先使用人工字幕
- 自动回退到自动生成字幕
- 支持 Whisper AI 转录（无字幕时）
- 自动去重字幕行
- 智能文件命名

**使用示例**：
```
"下载这个 YouTube 视频的字幕"
"获取视频的转录文本"
"转录这个 YouTube 视频"
```

 [详细文档](./youtube-transcript/README.md)

---

### 3. write_code
**功能**：代码编写辅助

 [详细文档](./write_code/SKILL.md)

---

### 4. write_design
**功能**：详细设计文档编写

 [详细文档](./write_design/)

---

##  如何使用

### 方式 1：直接对话
直接向 GitHub Copilot 提出请求，Agent 会自动识别并使用相应的 skill：

```
"下载这个 YouTube 视频的字幕"
"帮我下载这个视频的 720p 版本"
"转录视频内容"
```

### 方式 2：查看文档
每个 skill 都包含详细的 README.md 和 SKILL.md 文档：
- **README.md**：中文使用说明，面向用户
- **SKILL.md**：技术规范，面向 AI Agent

##  目录结构

```
.github/skills/
 video-downloader/
    SKILL.md              # Skill 定义
    README.md             # 使用文档（中文）
    scripts/
        download_video.py # 下载脚本
 youtube-transcript/
    SKILL.md              # Skill 定义
    README.md             # 使用文档（中文）
 write_code/
    SKILL.md
 write_design/
```

##  技术依赖

### video-downloader
- Python 3.x
- yt-dlp（自动安装）

### youtube-transcript
- Python 3.x
- yt-dlp（自动安装）
- OpenAI Whisper（可选，用于无字幕视频转录）

##  Skill 开发指南

### Skill 结构
每个 skill 应包含：

1. **SKILL.md**（必需）：
   - YAML frontmatter（name 和 description）
   - 使用场景说明
   - 详细的技术文档

2. **README.md**（推荐）：
   - 中文使用说明
   - 功能特性
   - 使用示例
   - 注意事项

3. **scripts/**（可选）：
   - 可执行脚本
   - 工具函数

### 创建新 Skill

1. 在 `.github/skills/` 下创建新目录
2. 创建 SKILL.md（包含 YAML frontmatter）
3. 创建 README.md（中文文档）
4. 添加必要的脚本和资源
5. 更新本文档

##  参考资源

- [Anthropic Claude Skills](https://github.com/anthropics/skills)
- [ComposioHQ Awesome Claude Skills](https://github.com/ComposioHQ/awesome-claude-skills)
- [Tapestry Skills for Claude Code](https://github.com/michalparkola/tapestry-skills-for-claude-code)

##  贡献

欢迎贡献新的 skills 或改进现有 skills！

##  许可证

各 skill 遵循各自的许可证，详见各 skill 目录。
