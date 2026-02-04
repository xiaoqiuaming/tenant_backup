const pptxgen = require('pptxgenjs');
const sharp = require('sharp');
const path = require('path');

// Create gradient backgrounds
async function createGradients() {
  const svg1 = `<svg xmlns="http://www.w3.org/2000/svg" width="1920" height="1080">
    <defs>
      <linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" style="stop-color:#667eea"/>
        <stop offset="100%" style="stop-color:#764ba2"/>
      </linearGradient>
    </defs>
    <rect width="100%" height="100%" fill="url(#g)"/>
  </svg>`;

  const svg2 = `<svg xmlns="http://www.w3.org/2000/svg" width="960" height="1080">
    <defs>
      <linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" style="stop-color:#667eea"/>
        <stop offset="100%" style="stop-color:#764ba2"/>
      </linearGradient>
    </defs>
    <rect width="100%" height="100%" fill="url(#g)"/>
  </svg>`;

  await sharp(Buffer.from(svg1)).png().toFile('gradient-bg1.png');
  await sharp(Buffer.from(svg2)).png().toFile('gradient-bg2.png');
}

async function createPresentation() {
  await createGradients();

  const pptx = new pptxgen();
  pptx.layout = 'LAYOUT_16x9';
  pptx.author = 'Multi-Tenant Team';
  pptx.title = '多租户资源隔离系统';

  // Slide 1: Title
  const slide1 = pptx.addSlide();
  slide1.background = { path: 'gradient-bg1.png' };
  slide1.addText('多租户资源隔离系统', {
    x: 0.5, y: 1.5, w: 9, h: 1.2,
    fontSize: 56, bold: true, color: 'FFFFFF',
    align: 'center', valign: 'middle',
    shadow: { type: 'outer', blur: 4, offset: 2, angle: 45, color: '000000', opacity: 0.3 }
  });
  slide1.addText('高性能数据库资源管理方案', {
    x: 1, y: 3, w: 8, h: 0.6,
    fontSize: 28, color: 'FFFFFF',
    align: 'center', valign: 'middle'
  });

  // Slide 2: Features
  const slide2 = pptx.addSlide();
  slide2.background = { color: 'F8F9FA' };
  
  slide2.addShape(pptx.shapes.RECTANGLE, {
    x: 0, y: 0, w: 5, h: 5.625,
    fill: { color: 'FFFFFF' },
    line: { type: 'none' }
  });
  
  slide2.addText('核心功能', {
    x: 0.5, y: 0.5, w: 4, h: 0.6,
    fontSize: 36, bold: true, color: '667eea'
  });

  const features = [
    { title: 'CPU资源隔离', desc: '基于cgroup实现精确的CPU配额管理', y: 1.4 },
    { title: '内存管理', desc: '动态内存分配与实时监控', y: 2.3 },
    { title: '磁盘I/O控制', desc: '智能磁盘配额与性能优化', y: 3.2 },
    { title: '租户认证', desc: '安全的多租户身份验证机制', y: 4.1 }
  ];

  features.forEach(f => {
    slide2.addText(f.title, {
      x: 0.5, y: f.y, w: 4, h: 0.3,
      fontSize: 18, bold: true, color: '333333'
    });
    slide2.addText(f.desc, {
      x: 0.5, y: f.y + 0.35, w: 4, h: 0.25,
      fontSize: 14, color: '666666'
    });
  });

  const stats = [
    { num: '99.9%', label: '资源隔离准确率', y: 1.2 },
    { num: '<10ms', label: '平均响应延迟', y: 2.5 },
    { num: '1000+', label: '并发租户支持', y: 3.8 }
  ];

  stats.forEach(s => {
    slide2.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
      x: 5.5, y: s.y, w: 4, h: 1,
      fill: { color: '667eea' },
      line: { type: 'none' },
      rectRadius: 0.15
    });
    slide2.addText(s.num, {
      x: 5.5, y: s.y + 0.15, w: 4, h: 0.5,
      fontSize: 48, bold: true, color: 'FFFFFF',
      align: 'center'
    });
    slide2.addText(s.label, {
      x: 5.5, y: s.y + 0.65, w: 4, h: 0.25,
      fontSize: 16, color: 'FFFFFF',
      align: 'center'
    });
  });

  // Slide 3: Architecture
  const slide3 = pptx.addSlide();
  slide3.background = { color: 'F8F9FA' };
  
  slide3.addShape(pptx.shapes.RECTANGLE, {
    x: 0, y: 0, w: 10, h: 1,
    fill: { color: '667eea' },
    line: { type: 'none' }
  });
  slide3.addText('系统架构', {
    x: 0.5, y: 0.3, w: 9, h: 0.5,
    fontSize: 36, bold: true, color: 'FFFFFF'
  });

  const layers = [
    { title: '服务层', desc: 'SqlServer、DataServer、TransServer、AdminServer', y: 1.3 },
    { title: '核心层', desc: 'TenantManager、ResourceManager', y: 2.3 },
    { title: '通用层', desc: 'ConfigManager、Utils', y: 3.3 }
  ];

  layers.forEach(l => {
    slide3.addShape(pptx.shapes.RECTANGLE, {
      x: 0.5, y: l.y, w: 4.5, h: 0.8,
      fill: { color: 'FFFFFF' },
      line: { type: 'solid', color: '667eea', width: 6, dashType: 'solid' },
      rectRadius: 0.05,
      shadow: { type: 'outer', blur: 8, offset: 2, angle: 45, color: '000000', opacity: 0.1 }
    });
    slide3.addText(l.title, {
      x: 0.6, y: l.y + 0.1, w: 4.3, h: 0.3,
      fontSize: 18, bold: true, color: '667eea'
    });
    slide3.addText(l.desc, {
      x: 0.6, y: l.y + 0.45, w: 4.3, h: 0.25,
      fontSize: 13, color: '666666'
    });
  });

  slide3.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x: 5.5, y: 1.3, w: 4, h: 2.8,
    fill: { color: 'FFFFFF' },
    line: { type: 'none' },
    rectRadius: 0.1,
    shadow: { type: 'outer', blur: 8, offset: 2, angle: 45, color: '000000', opacity: 0.1 }
  });
  
  slide3.addText('技术栈', {
    x: 5.5, y: 1.5, w: 4, h: 0.3,
    fontSize: 20, bold: true, color: '333333',
    align: 'center'
  });

  const techStack = [
    { label: '编程语言', value: 'C++17', y: 2 },
    { label: '资源隔离', value: 'Linux cgroup v2', y: 2.4 },
    { label: '存储引擎', value: 'LSM-tree', y: 2.8 },
    { label: '一致性', value: 'RAFT consensus', y: 3.2 },
    { label: '测试框架', value: 'GoogleTest', y: 3.6 }
  ];

  techStack.forEach(t => {
    slide3.addText(t.label, {
      x: 5.8, y: t.y, w: 3.4, h: 0.2,
      fontSize: 14, bold: true, color: '667eea'
    });
    slide3.addText(t.value, {
      x: 5.8, y: t.y + 0.22, w: 3.4, h: 0.18,
      fontSize: 13, color: '666666'
    });
  });

  await pptx.writeFile({ fileName: '多租户资源隔离系统.pptx' });
  console.log('演示文稿创建成功！');
}

createPresentation().catch(console.error);
