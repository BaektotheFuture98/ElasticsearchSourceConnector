from pathlib import Path
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.platypus import (
    HRFlowable,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

OUT_PATH = Path('/Users/seonminbaek/Documents/경력기술서_백선민_가독성개선.pdf')
ALT_OUT_PATH = Path('/Users/seonminbaek/IdeaProjects/ElasticsearchSourceConnector/output/pdf/경력기술서_백선민_가독성개선.pdf')
FONT_PATH = '/System/Library/Fonts/Supplemental/AppleGothic.ttf'
FONT_NAME = 'AppleGothic'

PROFILE = {
    'name_ko': '백선민',
    'name_en': 'Baek Seonmin',
    'email': 'sun4461637@naver.com',
    'github': 'https://github.com/BaektotheFuture98',
    'phone': '010-2828-1637',
    'summary': (
        'Elasticsearch 기반 검색 시스템과 데이터 파이프라인을 설계·운영해 왔으며, '
        'Airflow, Kafka, Prometheus/Grafana를 활용해 운영 가시성과 자동화를 개선한 경험이 있습니다.'
    ),
}

STACK_ROWS = [
    ('언어 / 쿼리', 'Java, Python, SQL'),
    ('검색엔진', 'Elasticsearch - 클러스터 운영, 색인/샤드 관리, 검색 파이프라인 설계, ILM 검증'),
    ('데이터 인프라', 'Kafka, Airflow'),
    ('모니터링', 'Prometheus, Grafana, Kibana'),
    ('CI / CD', 'Jenkins, Git'),
]

HIGHLIGHTS = [
    '색인 처리 속도 65% 개선 (40초 -> 16초)',
    '일 400만건 데이터 이동 자동화 검증',
    'Prometheus/Grafana 기반 검색 장애 진단 체계 구축',
    'Airflow 전환으로 재수집 파이프라인 운영 가시성 확보',
]

OTHER_WORK = [
    'Quettai 플랫폼 데이터 파이프라인 개발',
    'Jenkins 기반 CI/CD 파이프라인 구성',
    '고객사 맞춤 데이터 추출 프로세스 개발 (Excel Export)',
    'Elasticsearch 클러스터 운영 - 데이터 이동, 노드 확장',
]

PROJECTS = [
    {
        'title': '반응량 재수집 프로세스 운영 전환',
        'meta': '2026.02 | 2인 | 웹 MVP 제외 대부분 작업 담당',
        'summary': '분석 플랫폼 스키마 변경에 맞춰 댓글/좋아요 재수집 프로세스를 Airflow 기반으로 재구성한 사내 운영용 프로젝트.',
        'problem': [
            'Quettai 업그레이드로 기존 재수집 프로세스의 데이터 스키마와 타입이 변경되어 전면 마이그레이션이 필요했습니다.',
            'Crontab 기반 운영 환경에서는 실행 상태 확인과 장애 대응이 어려웠습니다.',
        ],
        'solution': [
            'JAR 실행 방식의 레거시 프로세스를 기능별 태스크로 분해해 Airflow DAG로 재설계했습니다.',
            'DB 직접 접근 리스크에 대비해 롤백 DAG를 별도로 구현하고, DAG 실패 시 텔레그램 알림이 가도록 구성했습니다.',
            '인계받은 웹 MVP의 오류 수정과 예외 처리를 보강해 운영 안정성을 높였습니다.',
        ],
        'impact': [
            '재수집 작업의 진행 상태를 시각적으로 확인할 수 있게 되어 블랙박스형 운영 구조를 해소했습니다.',
            '장애 방치 시간을 줄이고 복구 절차를 명확히 해 운영 투명성을 높였습니다.',
        ],
        'tech': 'Airflow, Python, Java Spring Boot, Copilot',
    },
    {
        'title': 'Git/Jenkins 기반 Kafka 데이터 파이프라인 자동 배포 PoC',
        'meta': '2025.09 - 2025.12 | 1인',
        'summary': '배치 중심 데이터 처리 구조를 이벤트 스트리밍 방식으로 전환하기 위해 Kafka 기반 파이프라인과 자동 배포 체계를 검증한 PoC.',
        'problem': [
            '데이터 이관, API, 엑셀 추출 등 다양한 요청을 기존 배치 아키텍처로 처리하는 데 한계가 있었습니다.',
            '재처리와 확장성을 고려한 스트리밍 도입이 필요했습니다.',
        ],
        'solution': [
            'Kafka, Schema Registry, Kafka Connector 기반 스트리밍 환경을 구성했습니다.',
            'Prometheus, Grafana, Kafka Exporter로 처리량, LAG, 오프셋을 확인할 수 있는 모니터링 환경을 만들었습니다.',
            'Git/Jenkins 파이프라인을 도입해 YAML 설정만으로 ES -> MySQL 데이터 파이프라인을 생성할 수 있도록 구성했습니다.',
        ],
        'impact': [
            '개발자 개입 없이 설정 파일만으로 데이터 추출이 가능해져 요청 처리 지연을 줄였습니다.',
            '재사용 가능한 파이프라인 운영 기반을 확보했고, 사내 및 대외 서비스 확장을 이어가고 있습니다.',
        ],
        'tech': 'Kafka, Schema Registry, Kafka Connector, Logstash, Prometheus, Grafana, Elasticsearch, MySQL, Jenkins, Git, FastAPI',
    },
    {
        'title': '연관어/감성 사전 데이터 전처리 파이프라인 최적화',
        'meta': '2025.01 | 1인',
        'summary': '연관어·감성 사전을 활용한 Elasticsearch 재색인 과정의 병목을 분석하고 스레드 처리 방식을 개선한 작업.',
        'problem': [
            '사전 기반 텍스트 매칭과 재색인 과정에서 처리 속도 병목이 발생했습니다.',
            '스레드 풀 설정에 따라 실제 성능이 어떻게 달라지는지 검증이 필요했습니다.',
        ],
        'solution': [
            'JVM 모니터링으로 worker thread 유휴 시간이 병목의 주요 원인임을 확인했습니다.',
            'FixedThreadPool과 CompletableFuture로 작업 할당 방식을 재구성하고, Semaphore로 작업 큐를 제어했습니다.',
        ],
        'impact': [
            '색인 처리 시간을 40초에서 16초로 단축해 최대 65% 성능 개선을 달성했습니다.',
        ],
        'tech': 'Java, Elasticsearch, MySQL',
    },
    {
        'title': '검색 인덱스 라이프사이클 관리 자동화',
        'meta': '2024.09 | 1인',
        'summary': 'Elasticsearch 스토리지 티어 불균형 문제를 해결하기 위해 ILM 적용 가능성과 운영 적합성을 검증한 PoC.',
        'problem': [
            '클러스터 내 스토리지 티어 간 불균형이 발생했고, Hot -> Warm 구조로 자동 관리가 가능한지 검증이 필요했습니다.',
            '서비스 특성상 과거 데이터에도 update가 발생해 일반적인 Warm phase 운영 제약을 함께 검토해야 했습니다.',
        ],
        'solution': [
            '테스트 클러스터에 hot, warm, cold 노드를 구성하고 Rollover 기반 ILM 정책과 인덱스 템플릿을 설계했습니다.',
            '날짜 기반 인덱스 네이밍 규칙과 Warm phase의 수정 불가 제약 때문에 운영 환경에 그대로 적용하기 어렵다는 점을 검증했습니다.',
            '대안으로 Curator, Shell Script, Crontab을 조합한 우회 관리 프로세스를 제시했습니다.',
        ],
        'impact': [
            'ILM의 구조적 제약을 명확히 정리했고, 현행 인덱스 규칙에 맞는 대안을 통해 일 400만건 데이터 이동 자동화를 검증했습니다.',
        ],
        'tech': 'Java, Crontab, Shell Script, Curator, Elasticsearch, Kibana',
    },
    {
        'title': '검색 쿼리 메트릭 수집 및 장애 진단 시스템 구축',
        'meta': '2025.06 | 2인',
        'summary': 'Circuit Breaker로 인한 데이터 노드 장애 원인을 추적하기 위해 검색 쿼리 메타데이터 수집과 대시보드 기반 진단 체계를 구축한 프로젝트.',
        'problem': [
            'Elasticsearch Cluster 일부 데이터 노드가 종종 내려가는 문제가 있었고, 어떤 쿼리와 호출 주체가 원인인지 식별이 어려웠습니다.',
        ],
        'solution': [
            'Java 기반 로그 수집 Agent로 쿼리, 실행시간, 요청일시 등의 메타데이터를 색인했습니다.',
            'Crontab으로 Python 집계 프로세스를 주기 실행해 결과를 .prom 파일로 변환했습니다.',
            'Prometheus와 Grafana 대시보드로 Top-N 검색어와 사용자 요청량을 시각화했습니다.',
        ],
        'impact': [
            '검색 품질 및 시스템 안정성을 높였고, Kibana와 Grafana를 통해 병목 유발 쿼리와 장애 원인 분석 시간을 단축했습니다.',
        ],
        'tech': 'Python, Crontab, Java, Node Exporter, Prometheus, Grafana, Elasticsearch',
    },
    {
        'title': 'NiFi Parameter Contexts 기반 Thread/Task 최적화 PoC',
        'meta': '2024.11 | 2인',
        'summary': 'NiFi 도입 가능성을 검토하며 Parameter Contexts와 스레드 설정값이 처리량에 미치는 영향을 실험한 PoC.',
        'problem': [
            '날짜 기반 검색 인덱스를 동적으로 운용할 수 있는지, 그리고 Time Driven Threads/Concurrent Tasks 설정이 성능에 어떤 영향을 주는지 검증이 필요했습니다.',
        ],
        'solution': [
            'NiFi REST API를 이용해 Parameter Context의 날짜 값을 동적으로 변경하는 실험을 진행했습니다.',
            'InvokeHTTP와 ReplaceText 조합, Regex와 EL(Expression Language)로 GET/POST 기반 업데이트 흐름을 구성했습니다.',
            'Thread 수와 Schedule Duration 변화에 따른 CPU 점유율, 스레드 수, FlowFile 처리량을 관찰했습니다.',
        ],
        'impact': [
            '검색 인덱스 기반 데이터 파이프라인의 동적 운용 가능성을 확인했고, NiFi 설정값과 처리량의 상관관계를 파악했습니다.',
        ],
        'tech': 'NiFi, Java, Elasticsearch',
    },
]


def register_fonts():
    pdfmetrics.registerFont(TTFont(FONT_NAME, FONT_PATH))


def styles():
    sample = getSampleStyleSheet()
    sample['Normal'].fontName = FONT_NAME
    sample['Normal'].fontSize = 10.2
    sample['Normal'].leading = 15
    sample['Normal'].textColor = colors.HexColor('#1F2937')

    sample.add(ParagraphStyle(
        name='TitleKo', fontName=FONT_NAME, fontSize=24, leading=29,
        textColor=colors.HexColor('#0F172A'), alignment=TA_CENTER, spaceAfter=4,
    ))
    sample.add(ParagraphStyle(
        name='TitleEn', fontName=FONT_NAME, fontSize=11, leading=14,
        textColor=colors.HexColor('#475569'), alignment=TA_CENTER, spaceAfter=12,
    ))
    sample.add(ParagraphStyle(
        name='Contact', fontName=FONT_NAME, fontSize=9.5, leading=13,
        textColor=colors.HexColor('#334155'), alignment=TA_CENTER,
    ))
    sample.add(ParagraphStyle(
        name='SectionTitle', fontName=FONT_NAME, fontSize=13, leading=17,
        textColor=colors.HexColor('#1D4ED8'), spaceBefore=6, spaceAfter=6,
    ))
    sample.add(ParagraphStyle(
        name='SummaryBox', fontName=FONT_NAME, fontSize=10.5, leading=16,
        textColor=colors.HexColor('#0F172A'),
    ))
    sample.add(ParagraphStyle(
        name='ProjectTitle', fontName=FONT_NAME, fontSize=15, leading=20,
        textColor=colors.HexColor('#0F172A'), spaceAfter=2,
    ))
    sample.add(ParagraphStyle(
        name='Meta', fontName=FONT_NAME, fontSize=9.2, leading=13,
        textColor=colors.HexColor('#64748B'), spaceAfter=8,
    ))
    sample.add(ParagraphStyle(
        name='ProjectSummary', fontName=FONT_NAME, fontSize=10.3, leading=15,
        textColor=colors.HexColor('#111827'), spaceAfter=8,
    ))
    sample.add(ParagraphStyle(
        name='Label', fontName=FONT_NAME, fontSize=9.5, leading=12,
        textColor=colors.HexColor('#1D4ED8'), spaceBefore=4, spaceAfter=3,
    ))
    sample.add(ParagraphStyle(
        name='BulletBody', fontName=FONT_NAME, fontSize=9.7, leading=14,
        textColor=colors.HexColor('#1F2937'), leftIndent=10,
    ))
    sample.add(ParagraphStyle(
        name='Tech', fontName=FONT_NAME, fontSize=9.4, leading=13,
        textColor=colors.HexColor('#334155'), spaceBefore=6,
    ))
    sample.add(ParagraphStyle(
        name='Small', fontName=FONT_NAME, fontSize=9.5, leading=14,
        textColor=colors.HexColor('#1F2937'),
    ))
    return sample


def on_page(canvas, doc):
    canvas.saveState()
    canvas.setStrokeColor(colors.HexColor('#CBD5E1'))
    canvas.setLineWidth(0.6)
    canvas.line(doc.leftMargin, A4[1] - 16 * mm, A4[0] - doc.rightMargin, A4[1] - 16 * mm)
    canvas.setFont(FONT_NAME, 8.5)
    canvas.setFillColor(colors.HexColor('#64748B'))
    canvas.drawRightString(A4[0] - doc.rightMargin, 10 * mm, f'{canvas.getPageNumber()}')
    canvas.restoreState()


def bullet_paragraph(text, style):
    return Paragraph(f'- {text}', style)


def project_block(project, st):
    block = []
    block.append(Paragraph(project['title'], st['ProjectTitle']))
    block.append(Paragraph(project['meta'], st['Meta']))
    block.append(Paragraph(project['summary'], st['ProjectSummary']))
    for label, key in [('문제', 'problem'), ('해결', 'solution'), ('성과', 'impact')]:
        block.append(Paragraph(label, st['Label']))
        for item in project[key]:
            block.append(bullet_paragraph(item, st['BulletBody']))
            block.append(Spacer(1, 1.5 * mm))
    block.append(Paragraph(f'기술 | {project["tech"]}', st['Tech']))
    block.append(Spacer(1, 2.5 * mm))
    block.append(HRFlowable(width='100%', thickness=0.6, color=colors.HexColor('#CBD5E1')))
    block.append(Spacer(1, 5 * mm))
    return KeepTogether(block)


def summary_box(text, st, width):
    t = Table([[Paragraph(text, st['SummaryBox'])]], colWidths=[width])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#EFF6FF')),
        ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#BFDBFE')),
        ('LEFTPADDING', (0, 0), (-1, -1), 12),
        ('RIGHTPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
    ]))
    return t


def stack_table(st, width):
    rows = []
    for label, detail in STACK_ROWS:
        rows.append([
            Paragraph(label, st['Small']),
            Paragraph(detail, st['Small']),
        ])
    table = Table(rows, colWidths=[34 * mm, width - 34 * mm])
    table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), FONT_NAME),
        ('FONTSIZE', (0, 0), (-1, -1), 9.4),
        ('LEADING', (0, 0), (-1, -1), 13),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('BACKGROUND', (0, 0), (-1, -1), colors.white),
        ('LINEBELOW', (0, 0), (-1, -1), 0.35, colors.HexColor('#E2E8F0')),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.HexColor('#0F172A')),
        ('TEXTCOLOR', (1, 0), (1, -1), colors.HexColor('#334155')),
    ]))
    return table


def simple_bullets(items, st):
    flowables = []
    for item in items:
        flowables.append(bullet_paragraph(item, st['BulletBody']))
        flowables.append(Spacer(1, 1.5 * mm))
    return flowables


def build_pdf():
    register_fonts()
    st = styles()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ALT_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(OUT_PATH),
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=22 * mm,
        bottomMargin=16 * mm,
        title='경력기술서 - 백선민',
        author='Codex',
    )

    story = []
    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph('경력기술서', st['TitleKo']))
    story.append(Paragraph(f"{PROFILE['name_ko']} | {PROFILE['name_en']}", st['TitleEn']))
    story.append(Paragraph(
        f"{PROFILE['email']} &nbsp;&nbsp;|&nbsp;&nbsp; {PROFILE['github']} &nbsp;&nbsp;|&nbsp;&nbsp; {PROFILE['phone']}",
        st['Contact'],
    ))
    story.append(Spacer(1, 6 * mm))
    story.append(summary_box(PROFILE['summary'], st, doc.width))
    story.append(Spacer(1, 7 * mm))

    story.append(Paragraph('핵심 역량', st['SectionTitle']))
    story.extend(simple_bullets([
        'Elasticsearch 클러스터 운영, 인덱스 설계, 재색인 파이프라인 최적화 경험',
        'Airflow, Kafka 기반 데이터 파이프라인 설계 및 운영 자동화 경험',
        'Prometheus, Grafana, Kibana를 활용한 검색 로그 기반 모니터링 구축 경험',
        '운영 환경 제약을 검증하고 우회 대안을 설계하는 PoC 및 실무 적용 경험',
    ], st))
    story.append(Spacer(1, 2 * mm))

    story.append(Paragraph('기술 스택', st['SectionTitle']))
    story.append(stack_table(st, doc.width))
    story.append(Spacer(1, 5 * mm))

    story.append(Paragraph('주요 성과', st['SectionTitle']))
    story.extend(simple_bullets(HIGHLIGHTS, st))
    story.append(Spacer(1, 2 * mm))

    story.append(Paragraph('기타 업무 경험', st['SectionTitle']))
    story.extend(simple_bullets(OTHER_WORK, st))

    story.append(PageBreak())

    for idx, project in enumerate(PROJECTS, start=1):
        story.append(project_block(project, st))
        if idx in (2, 4):
            story.append(PageBreak())

    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    ALT_OUT_PATH.write_bytes(OUT_PATH.read_bytes())
    print(OUT_PATH)
    print(ALT_OUT_PATH)


if __name__ == '__main__':
    build_pdf()
