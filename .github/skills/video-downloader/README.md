# YouTube Video Downloader Skill

这是一个用于下载 YouTube 视频的 GitHub Copilot Agent Skill，移植自 [ComposioHQ/awesome-claude-skills](https://github.com/ComposioHQ/awesome-claude-skills/tree/master/video-downloader)。

## 功能特性

- 支持多种视频质量选择（best, 1080p, 720p, 480p, 360p, worst）
- 支持多种视频格式（mp4, webm, mkv）
- 支持仅下载音频（MP3 格式）
- 自动安装依赖（yt-dlp）
- 显示视频信息（标题、时长、上传者）
- 自定义输出目录

## 使用方法

### 基本用法

下载视频（默认最佳质量 MP4 格式）：
```bash
python .github/skills/video-downloader/scripts/download_video.py "https://www.youtube.com/watch?v=VIDEO_ID"
```

### 高级用法

**指定视频质量：**
```bash
python .github/skills/video-downloader/scripts/download_video.py "VIDEO_URL" -q 720p
```

**指定输出格式：**
```bash
python .github/skills/video-downloader/scripts/download_video.py "VIDEO_URL" -f webm
```

**仅下载音频（MP3）：**
```bash
python .github/skills/video-downloader/scripts/download_video.py "VIDEO_URL" -a
```

**自定义输出目录：**
```bash
python .github/skills/video-downloader/scripts/download_video.py "VIDEO_URL" -o ./downloads
```

**组合使用：**
```bash
python .github/skills/video-downloader/scripts/download_video.py "VIDEO_URL" -q 1080p -f mp4 -o ./videos
```

## 参数说明

| 参数 | 简写 | 说明 | 默认值 | 可选值 |
|------|------|------|--------|--------|
| url | - | YouTube 视频 URL | 必填 | - |
| --output | -o | 输出目录 | /mnt/user-data/outputs | 任意路径 |
| --quality | -q | 视频质量 | best | best, 1080p, 720p, 480p, 360p, worst |
| --format | -f | 视频格式 | mp4 | mp4, webm, mkv |
| --audio-only | -a | 仅下载音频 | false | - |

## 工作原理

1. **自动安装依赖**：首次运行时会自动检测并安装 yt-dlp
2. **获取视频信息**：在下载前获取视频的标题、时长、上传者等信息
3. **智能流选择**：根据指定的质量和格式选择最佳视频和音频流
4. **自动合并**：当需要时自动合并视频和音频流
5. **友好提示**：实时显示下载进度和完成状态

## 技术依赖

- Python 3.x
- yt-dlp（自动安装）

## 注意事项

- 默认输出目录为 `/mnt/user-data/outputs/`
- 文件名会根据视频标题自动生成
- 默认只下载单个视频（不下载播放列表）
- 高质量视频下载时间较长，占用空间较大
- 请遵守相关版权法律，仅下载允许下载的内容

## 示例

### 示例 1：下载 1080p MP4 视频
```bash
python .github/skills/video-downloader/scripts/download_video.py \
  "https://www.youtube.com/watch?v=dQw4w9WgXcQ" -q 1080p
```

### 示例 2：下载音频为 MP3
```bash
python .github/skills/video-downloader/scripts/download_video.py \
  "https://www.youtube.com/watch?v=dQw4w9WgXcQ" -a
```

### 示例 3：下载 720p WebM 到自定义目录
```bash
python .github/skills/video-downloader/scripts/download_video.py \
  "https://www.youtube.com/watch?v=dQw4w9WgXcQ" \
  -q 720p -f webm -o ./my_videos
```

## 在 Copilot Agent 中使用

当你向 GitHub Copilot 提出以下请求时，它会自动使用这个 skill：

- "帮我下载这个 YouTube 视频"
- "把这个视频下载成 720p 的 MP4 格式"
- "下载这个视频的音频部分"
- "保存这个 YouTube 视频到本地"

Agent 会自动调用 `download_video.py` 脚本并传入适当的参数。

## 许可证

本 skill 移植自 ComposioHQ/awesome-claude-skills 项目，遵循原项目许可证。
