# YouTube Transcript Downloader Skill

这是一个用于下载 YouTube 视频字幕/转录文本的 GitHub Copilot Agent Skill，移植自 [michalparkola/tapestry-skills-for-claude-code](https://github.com/michalparkola/tapestry-skills-for-claude-code/tree/main/youtube-transcript)。

## 功能特性

- 自动检测并安装 yt-dlp
- 优先使用人工字幕（质量最佳）
- 自动回退到自动生成字幕
- 支持 Whisper AI 转录（无字幕时）
- 自动去重字幕行
- 智能文件命名（使用视频标题）
- 清理临时文件
- 多语言字幕支持

## 使用方法

### 基本用法

向 GitHub Copilot 提出以下请求：

```
"下载这个 YouTube 视频的字幕：https://www.youtube.com/watch?v=VIDEO_ID"
"获取这个视频的转录文本"
"下载 YouTube 视频的 captions"
"转录这个 YouTube 视频"
```

Agent 会自动：
1. 检查并安装 yt-dlp（如需要）
2. 列出可用的字幕
3. 尝试下载人工字幕
4. 如果不可用，下载自动生成字幕
5. 转换为纯文本并去重
6. 使用视频标题作为文件名
7. 清理临时文件

### 完整工作流程

1. **检查可用字幕**：
   ```bash
   yt-dlp --list-subs "YOUTUBE_URL"
   ```

2. **下载人工字幕（首选）**：
   ```bash
   yt-dlp --write-sub --skip-download --output "transcript" "YOUTUBE_URL"
   ```

3. **回退到自动生成字幕**：
   ```bash
   yt-dlp --write-auto-sub --skip-download --output "transcript" "YOUTUBE_URL"
   ```

4. **转换为纯文本（去重）**：
   ```bash
   python3 -c "
   import sys, re
   seen = set()
   with open('transcript.en.vtt', 'r') as f:
       for line in f:
           line = line.strip()
           if line and '-->' not in line:
               clean = re.sub('<[^>]*>', '', line)
               if clean and clean not in seen:
                   print(clean)
                   seen.add(clean)
   " > transcript.txt
   ```

## 下载策略

### 优先级顺序

1. ✅ **人工字幕** - 最高质量，人工创建
2. 🔄 **自动生成字幕** - 通常可用，质量尚可
3. 🎤 **Whisper 转录** - 最后手段，需要用户确认

### Whisper 转录（仅在无字幕时使用）

当视频没有任何字幕时，系统会：

1. **显示文件大小并询问确认**：
   ```
   "没有可用字幕。我可以下载音频（约 X MB）并使用 Whisper 转录。是否继续？"
   ```

2. **检查 Whisper 安装**：
   ```bash
   pip install openai-whisper  # 需要 1-3GB 模型
   ```

3. **下载音频**：
   ```bash
   yt-dlp -x --audio-format mp3 --output "audio_%(id)s.%(ext)s" "URL"
   ```

4. **转录**：
   ```bash
   whisper audio.mp3 --model base --output_format vtt
   ```

5. **清理**（询问用户是否删除音频文件）

## Whisper 模型选项

| 模型 | 大小 | 准确度 | 推荐 |
|------|------|--------|------|
| tiny | ~1GB | 最低 | ❌ |
| base | ~1GB | 良好 | ✅ 推荐 |
| small | ~2GB | 更好 | ⭕ |
| medium | ~5GB | 很好 | ⭕ |
| large | ~10GB | 最佳 | ❌ |

**推荐使用 `base` 模型**，在准确度和速度之间取得良好平衡。

## 后处理

### 去重处理

YouTube 自动生成的字幕包含**重复行**，因为字幕以渐进方式显示（重叠时间戳）。系统会自动去除重复内容，同时保持原始顺序。

### 文件命名

使用视频标题作为文件名，自动清理特殊字符：
- 替换 `/` 为 `_`
- 替换 `:` 为 `-`
- 移除 `?` 和 `"`
- 示例：`How to Build a SaaS in 30 Days.txt`

## 输出格式

- **VTT 格式** (`.vtt`)：包含时间戳和格式，适合视频播放器
- **纯文本** (`.txt`)：仅文本内容，适合阅读或分析

## 错误处理

### 常见问题及解决方案

| 问题 | 解决方案 |
|------|----------|
| yt-dlp 未安装 | 自动安装（Homebrew/apt/pip） |
| 无可用字幕 | 提供 Whisper 转录选项 |
| 视频私有/受限 | 提示用户检查视频权限 |
| Whisper 安装失败 | 提供手动安装指南 |
| 下载中断 | 检查网络和磁盘空间 |
| 多语言字幕 | 使用 `--sub-langs en` 指定语言 |

### 最佳实践

- ✅ 下载前始终检查可用字幕
- ✅ 每步成功后再继续下一步
- ✅ 大文件下载前询问用户
- ✅ 处理后清理临时文件
- ✅ 提供清晰的进度反馈
- ✅ 友好处理错误信息

## 完整工作流示例

```bash
#!/bin/bash

VIDEO_URL="https://www.youtube.com/watch?v=dQw4w9WgXcQ"
VIDEO_TITLE=$(yt-dlp --print "%(title)s" "$VIDEO_URL" | tr '/' '_' | tr ':' '-')
OUTPUT_NAME="transcript_temp"

# 1. 检查 yt-dlp
if ! command -v yt-dlp &> /dev/null; then
    echo "安装 yt-dlp..."
    pip install yt-dlp
fi

# 2. 列出可用字幕
echo "检查可用字幕..."
yt-dlp --list-subs "$VIDEO_URL"

# 3. 尝试人工字幕
echo "尝试下载人工字幕..."
if yt-dlp --write-sub --skip-download --output "$OUTPUT_NAME" "$VIDEO_URL" 2>/dev/null; then
    echo "✓ 人工字幕下载成功！"
else
    # 4. 回退到自动生成字幕
    echo "尝试自动生成字幕..."
    if yt-dlp --write-auto-sub --skip-download --output "$OUTPUT_NAME" "$VIDEO_URL" 2>/dev/null; then
        echo "✓ 自动字幕下载成功！"
    else
        echo "⚠ 无可用字幕，需要 Whisper 转录"
        # Whisper 转录流程...
    fi
fi

# 5. 转换为纯文本并去重
VTT_FILE=$(ls ${OUTPUT_NAME}*.vtt | head -n 1)
python3 -c "
import sys, re
seen = set()
with open('$VTT_FILE', 'r') as f:
    for line in f:
        line = line.strip()
        if line and '-->' not in line:
            clean = re.sub('<[^>]*>', '', line)
            clean = clean.replace('&amp;', '&')
            if clean and clean not in seen:
                print(clean)
                seen.add(clean)
" > "${VIDEO_TITLE}.txt"

# 6. 清理
rm "$VTT_FILE"
echo "✓ 完成！保存到：${VIDEO_TITLE}.txt"
```

## 技术依赖

### 必需
- **yt-dlp**：YouTube 下载工具（自动安装）
- **Python 3**：用于后处理和去重

### 可选
- **Whisper**：AI 转录（无字幕时使用）
  ```bash
  pip install openai-whisper
  ```
- **ffmpeg**：音频处理（Whisper 需要）

## 安装说明

### macOS
```bash
brew install yt-dlp
# 可选：Whisper
pip3 install openai-whisper
```

### Linux (Ubuntu/Debian)
```bash
sudo apt update && sudo apt install -y yt-dlp
# 可选：Whisper
pip3 install openai-whisper
```

### Windows
```powershell
pip install yt-dlp
# 可选：Whisper
pip install openai-whisper
```

## 使用提示

- 大多数 YouTube 视频都有自动生成的英文字幕
- 文件名格式：`{output_name}.{language_code}.vtt`（如 `transcript.en.vtt`）
- 某些视频可能有多种语言选项
- 使用 `--sub-langs zh-Hans` 可指定中文简体字幕

## 在 Copilot Agent 中使用

当你向 GitHub Copilot 提出以下请求时，它会自动使用这个 skill：

- "下载这个 YouTube 视频的字幕"
- "获取视频的转录文本"
- "帮我提取 YouTube 视频的 captions"
- "转录这个视频内容"

Agent 会自动：
1. 检测并安装依赖
2. 选择最佳下载策略
3. 处理并去重文本
4. 使用视频标题命名文件
5. 清理临时文件

## 相关 Skills

- [video-downloader](../video-downloader/) - 下载完整视频文件
- 可与其他内容分析 skills 配合使用

## 许可证

本 skill 移植自 michalparkola/tapestry-skills-for-claude-code 项目，遵循原项目许可证。

## 致谢

- **yt-dlp**：优秀的 YouTube 下载工具
- **OpenAI Whisper**：先进的语音识别技术
- **Tapestry Skills**：原始 Claude skill 框架
