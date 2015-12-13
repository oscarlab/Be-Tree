// XGetoptTestDlg.cpp : implementation file
//

#include "stdafx.h"
#include "XGetoptTest.h"
#include "XGetoptTestDlg.h"
#include "argcargv.h"
#include "XGetopt.h"
#include "about.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

///////////////////////////////////////////////////////////////////////////////
// CXGetoptTestDlg dialog

BEGIN_MESSAGE_MAP(CXGetoptTestDlg, CDialog)
	//{{AFX_MSG_MAP(CXGetoptTestDlg)
	ON_WM_SYSCOMMAND()
	ON_WM_PAINT()
	ON_WM_QUERYDRAGICON()
	ON_BN_CLICKED(IDC_GETOPT, OnGetopt)
	//}}AFX_MSG_MAP
END_MESSAGE_MAP()

///////////////////////////////////////////////////////////////////////////////
// ctor
CXGetoptTestDlg::CXGetoptTestDlg(CWnd* pParent /*=NULL*/)
	: CDialog(CXGetoptTestDlg::IDD, pParent)
{
	//{{AFX_DATA_INIT(CXGetoptTestDlg)
	//}}AFX_DATA_INIT
	// Note that LoadIcon does not require a subsequent DestroyIcon in Win32
	m_hIcon = AfxGetApp()->LoadIcon(IDR_MAINFRAME);
}

///////////////////////////////////////////////////////////////////////////////
// DoDataExchange
void CXGetoptTestDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
	//{{AFX_DATA_MAP(CXGetoptTestDlg)
	DDX_Control(pDX, IDC_LIST, m_List);
	DDX_Control(pDX, IDC_COMMAND_LINE, m_CommandLine);
	//}}AFX_DATA_MAP
}

///////////////////////////////////////////////////////////////////////////////
// OnInitDialog
BOOL CXGetoptTestDlg::OnInitDialog()
{
	CDialog::OnInitDialog();

	// Add "About..." menu item to system menu.

	// IDM_ABOUTBOX must be in the system command range.
	ASSERT((IDM_ABOUTBOX & 0xFFF0) == IDM_ABOUTBOX);
	ASSERT(IDM_ABOUTBOX < 0xF000);

	CMenu* pSysMenu = GetSystemMenu(FALSE);
	if (pSysMenu != NULL)
	{
		CString strAboutMenu;
		strAboutMenu.LoadString(IDS_ABOUTBOX);
		if (!strAboutMenu.IsEmpty())
		{
			pSysMenu->AppendMenu(MF_SEPARATOR);
			pSysMenu->AppendMenu(MF_STRING, IDM_ABOUTBOX, strAboutMenu);
		}
	}

	// Set the icon for this dialog.  The framework does this automatically
	//  when the application's main window is not a dialog
	SetIcon(m_hIcon, TRUE);			// Set big icon
	SetIcon(m_hIcon, FALSE);		// Set small icon
	
	// set context menu for listbox
	m_List.SetContextMenuId(IDR_XLISTBOX);

	// set sample command line
	m_CommandLine.SetWindowText(_T("XGetoptTest -ab -c -C -d foo -e123 xyz"));	

	return TRUE;  // return TRUE  unless you set the focus to a control
}

///////////////////////////////////////////////////////////////////////////////
// OnSysCommand
void CXGetoptTestDlg::OnSysCommand(UINT nID, LPARAM lParam)
{
	if ((nID & 0xFFF0) == IDM_ABOUTBOX)
	{
		CAboutDlg dlgAbout;
		dlgAbout.DoModal();
	}
	else
	{
		CDialog::OnSysCommand(nID, lParam);
	}
}

// If you add a minimize button to your dialog, you will need the code below
//  to draw the icon.  For MFC applications using the document/view model,
//  this is automatically done for you by the framework.

///////////////////////////////////////////////////////////////////////////////
// OnPaint
void CXGetoptTestDlg::OnPaint() 
{
	if (IsIconic())
	{
		CPaintDC dc(this); // device context for painting

		SendMessage(WM_ICONERASEBKGND, (WPARAM) dc.GetSafeHdc(), 0);

		// Center icon in client rectangle
		int cxIcon = GetSystemMetrics(SM_CXICON);
		int cyIcon = GetSystemMetrics(SM_CYICON);
		CRect rect;
		GetClientRect(&rect);
		int x = (rect.Width() - cxIcon + 1) / 2;
		int y = (rect.Height() - cyIcon + 1) / 2;

		// Draw the icon
		dc.DrawIcon(x, y, m_hIcon);
	}
	else
	{
		CDialog::OnPaint();
	}
}

///////////////////////////////////////////////////////////////////////////////
// The system calls this to obtain the cursor to display while the user drags
//  the minimized window.
// OnQueryDragIcon
HCURSOR CXGetoptTestDlg::OnQueryDragIcon()
{
	return (HCURSOR) m_hIcon;
}

///////////////////////////////////////////////////////////////////////////////
// OnGetopt
void CXGetoptTestDlg::OnGetopt() 
{
	CString strCommandLine;
	m_CommandLine.GetWindowText(strCommandLine);

	m_List.AddLine(CXListBox::Blue, CXListBox::White, strCommandLine);

	// convert string to argv format
	int argc = _ConvertCommandLineToArgcArgv(strCommandLine);

	BOOL bSuccess = FALSE;

	if (argc <= 0)
		AfxMessageBox(_T("Please enter at least one argument."));
	else
		bSuccess = ProcessCommandLine(argc, _ppszArgv);

	if (bSuccess)
		m_List.AddLine(CXListBox::Green, CXListBox::White, 
			_T("\tProcessCommandLine return:  all options processed\n"));
	else
		m_List.AddLine(CXListBox::Red, CXListBox::White, 
			_T("\tProcessCommandLine return:  options had errors\n"));
}

///////////////////////////////////////////////////////////////////////////////
// ProcessCommandLine
//
// In non-dialog based app, this function would be in CWinApp module, and
// would be called from InitInstance() like this:
//          ProcessCommandLine(__argc, __argv);
//
BOOL CXGetoptTestDlg::ProcessCommandLine(int argc, TCHAR *argv[])
{
	for (int i = 0; i < argc; i++)
	{
		m_List.Printf(CXListBox::Black, CXListBox::White, 0,
			_T("\targv[%d]=<%s>\n"), i, argv[i]);
	}

	int c;
	optind = 0;		// this does not normally have to be done, but in
					// this app we may be calling getopt again

	// In the following loop you would set/unset any global command 
	// line flags and option arguments (usually in the CWinApp object) 
	// as each option was found in the command line.  
	//
	// Here you could also enforce any required order in the options -
	// for example, if option 'b' is seen, then option 'a' must be given
	// first.  However, this is unusual and probably not a good idea
	// in most apps.
	//
	// In general it is probably best to let ProcessCommandLine's caller
	// sort out the command line arguments that were used, and whether 
	// they are consistent.  In ProcessCommandLine, you want to save the
	// options and the arguments, doing any conversion (atoi, etc.) that
	// is necessary.
	//
	// Note in the optstring there are colons following the option letters
	// (d and e) that take arguments.  Also note that option letters are 
	// case sensitive.
	//
	// Normally you would have a case statement for each option letter.
	// In this demo app, the case statement for option 'f' has been
	// omitted on purpose, to show what will happen.

	while ((c = getopt(argc, argv, _T("abcCd:e:f"))) != EOF)
	{
		switch (c)
		{
			case _T('a'):
				m_List.AddLine(CXListBox::Black, CXListBox::White, 
					_T("\toption a\n"));
				break;
				
			case _T('b'):
				m_List.AddLine(CXListBox::Black, CXListBox::White, 
					_T("\toption b\n"));
				break;

			case _T('c'):
				m_List.AddLine(CXListBox::Black, CXListBox::White, 
					_T("\toption c\n"));
				break;

			case _T('C'):
				m_List.AddLine(CXListBox::Black, CXListBox::White, 
					_T("\toption C\n"));
				break;
				
			case _T('d'):
				m_List.Printf(CXListBox::Black, CXListBox::White, 0,
					_T("\toption d with value '%s'\n"), optarg);
				break;
				
			case _T('e'):
				m_List.Printf(CXListBox::Black, CXListBox::White, 0,
					_T("\toption e with value '%s'\n"), optarg);
				break;
				
			case _T('?'):		// illegal option - i.e., an option that was
							// not specified in the optstring

				// Note: you may choose to ignore illegal options

				m_List.Printf(CXListBox::Red, CXListBox::White, 0,
					_T("\tERROR:  illegal option %s\n"), argv[optind-1]);
				{
					CString str;
					str.LoadString(AFX_IDS_APP_TITLE);
					CString msg;
					msg = _T("Usage:  " + str + " -a -b -c -C -d AAA -e NNN -f");
					AfxMessageBox(msg);
				}
				return FALSE;
				break;
				
			default:		// legal option - it was specified in optstring,
							// but it had no "case" handler 

				// Note: you may choose not to make this an error

				m_List.Printf(CXListBox::Red, CXListBox::White, 0,
					_T("\tWARNING:  no handler for option %c\n"), c);
				return FALSE;
				break;
		}
	}

	if (optind < argc)
	{
		CString strArgs;
		strArgs = _T("");

		// In this loop you would save any extra arguments (e.g., filenames).
		while (optind < argc)
		{
			if (strArgs.IsEmpty())
				strArgs = _T("\tAdditional non-option arguments: ");
			strArgs += _T("<");
			strArgs += argv[optind];
			strArgs += _T("> ");
			optind++;
		}

		if (!strArgs.IsEmpty())
		{
			m_List.AddLine(CXListBox::Black, CXListBox::White, (LPCTSTR)strArgs);
		}
	}

	// all options processed, return success
	return TRUE;
}
